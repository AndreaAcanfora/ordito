package io.github.andreaacanfora.ordito.chaos;

import io.github.andreaacanfora.ordito.check.KvModel;
import io.github.andreaacanfora.ordito.check.KvModel.Op;
import io.github.andreaacanfora.ordito.check.KvModel.Reply;
import io.github.andreaacanfora.ordito.check.Linearizability;
import io.github.andreaacanfora.ordito.check.Operation;
import io.github.andreaacanfora.ordito.check.Result;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Starts a real cluster of ordito-server processes and attacks it while clients run operations, the way
 * Jepsen does: partitions (by cutting TCP proxies between nodes), {@code kill -9} and restarts, and
 * {@code SIGSTOP}/{@code SIGCONT} pauses. At the end it heals everything and checks the history the
 * clients observed for linearizability.
 */
public final class Main {

    private static final String USAGE = """
            ordito-chaos: Jepsen-style test of real ordito-server processes.

            Usage: java -jar ordito-chaos.jar --server <ordito-server.jar> [options]
              --nodes <n>             cluster size (default 3)
              --clients <n>           concurrent clients (default 8)
              --time <seconds>        how long to inject faults (default 60)
              --seed <n>              seed for the fault schedule and the operations
              --no-faults             run the workload without faults
              --unsafe-stale-reads    start the servers with the stale-reads bug, to see it caught
            """;

    private static final int KEYS = 4;
    private static final int CLIENT_TIMEOUT_MS = 2000;

    private final Path serverJar;
    private final int nodeCount;
    private final int clientCount;
    private final int seconds;
    private final boolean faults;
    private final boolean staleReads;
    private final Random random;
    private final Path work;

    private final int[] clientPorts;
    private final int[] raftPorts;
    private final Proxy[][] proxies;
    private final Process[] processes;
    private final boolean[] paused;
    private final long start = System.nanoTime();
    private final List<Operation<Op, Reply>> history = Collections.synchronizedList(new ArrayList<>());
    private final AtomicInteger opIds = new AtomicInteger();
    private final AtomicLong values = new AtomicLong();
    private final AtomicLong ok = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();
    private final AtomicLong unknown = new AtomicLong();
    private volatile boolean clientsRunning = true;

    private Main(Path serverJar, int nodes, int clients, int seconds, boolean faults, boolean staleReads, long seed)
            throws IOException {
        this.serverJar = serverJar;
        this.nodeCount = nodes;
        this.clientCount = clients;
        this.seconds = seconds;
        this.faults = faults;
        this.staleReads = staleReads;
        this.random = new Random(seed);
        this.work = Files.createTempDirectory("ordito-chaos");
        clientPorts = new int[nodes + 1];
        raftPorts = new int[nodes + 1];
        proxies = new Proxy[nodes + 1][nodes + 1];
        processes = new Process[nodes + 1];
        paused = new boolean[nodes + 1];
    }

    public static void main(String[] args) throws Exception {
        Path jar = null;
        int nodes = 3;
        int clients = 8;
        int seconds = 60;
        boolean faults = true;
        boolean stale = false;
        long seed = System.nanoTime();
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--server" -> jar = Path.of(args[++i]);
                case "--nodes" -> nodes = Integer.parseInt(args[++i]);
                case "--clients" -> clients = Integer.parseInt(args[++i]);
                case "--time" -> seconds = Integer.parseInt(args[++i]);
                case "--seed" -> seed = Long.parseLong(args[++i]);
                case "--no-faults" -> faults = false;
                case "--unsafe-stale-reads" -> stale = true;
                case "--help", "-h" -> {
                    System.out.print(USAGE);
                    return;
                }
                default -> {
                    System.err.println("unknown option " + args[i]);
                    System.err.print(USAGE);
                    System.exit(2);
                }
            }
        }
        if (jar == null || !Files.exists(jar)) {
            System.err.print(USAGE);
            System.exit(2);
        }
        System.exit(new Main(jar, nodes, clients, seconds, faults, stale, seed).run(seed));
    }

    private int run(long seed) throws Exception {
        log("seed %d: %d nodes, %d clients, %d s of %s%s", seed, nodeCount, clientCount, seconds,
                faults ? "faults" : "calm", staleReads ? ", servers started with --unsafe-stale-reads" : "");
        for (int i = 1; i <= nodeCount; i++) {
            clientPorts[i] = freePort();
            raftPorts[i] = freePort();
        }
        for (int from = 1; from <= nodeCount; from++) {
            for (int to = 1; to <= nodeCount; to++) {
                if (from != to) {
                    proxies[from][to] = new Proxy(freePort(), new InetSocketAddress("127.0.0.1", raftPorts[to]));
                }
            }
        }
        try {
            for (int i = 1; i <= nodeCount; i++) {
                startNode(i);
            }
            waitForLeader(15_000);
            List<Thread> clients = new ArrayList<>();
            for (int c = 1; c <= clientCount; c++) {
                int id = c;
                long clientSeed = random.nextLong();
                clients.add(Thread.ofVirtual().name("client-" + c).start(() -> clientLoop(id, new Random(clientSeed))));
            }
            long end = System.currentTimeMillis() + seconds * 1000L;
            while (System.currentTimeMillis() < end) {
                Thread.sleep(2000 + random.nextInt(3000));
                if (faults && System.currentTimeMillis() < end) {
                    nemesis();
                }
            }
            log("faults stopped; healing the network and restarting everything");
            healAll();
            Thread.sleep(5000); // the cluster must recover while clients keep going
            clientsRunning = false;
            for (Thread t : clients) {
                t.join();
            }
            return verdict();
        } finally {
            for (int i = 1; i <= nodeCount; i++) {
                if (processes[i] != null) {
                    resume(i);
                    processes[i].destroyForcibly();
                }
            }
            for (Proxy[] row : proxies) {
                for (Proxy p : row) {
                    if (p != null) {
                        p.close();
                    }
                }
            }
        }
    }

    // ---- the cluster ----

    private void startNode(int id) throws IOException {
        StringBuilder cluster = new StringBuilder();
        StringBuilder via = new StringBuilder();
        for (int i = 1; i <= nodeCount; i++) {
            cluster.append(i == 1 ? "" : ",").append(i).append("=127.0.0.1:").append(clientPorts[i]).append(':')
                    .append(raftPorts[i]);
            if (i != id) {
                via.append(via.isEmpty() ? "" : ",").append(i).append("=127.0.0.1:").append(proxyPort(id, i));
            }
        }
        List<String> cmd = new ArrayList<>(List.of(javaBinary(), "-jar", serverJar.toString(), "--id", Integer.toString(id),
                "--cluster", cluster.toString(), "--dir", work.resolve("n" + id).toString(), "--tick-ms", "10"));
        if (!via.isEmpty()) {
            cmd.add("--peer-via");
            cmd.add(via.toString());
        }
        if (staleReads) {
            cmd.add("--unsafe-stale-reads");
        }
        File out = work.resolve("n" + id + ".log").toFile();
        processes[id] = new ProcessBuilder(cmd).redirectErrorStream(true)
                .redirectOutput(ProcessBuilder.Redirect.appendTo(out)).start();
        paused[id] = false;
    }

    private int proxyPort(int from, int to) {
        return proxies[from][to].port();
    }

    private void kill(int id) {
        resume(id);
        processes[id].destroyForcibly();
        try {
            processes[id].waitFor();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void pause(int id) {
        signal(id, "STOP");
        paused[id] = true;
    }

    private void resume(int id) {
        if (paused[id]) {
            signal(id, "CONT");
            paused[id] = false;
        }
    }

    private void signal(int id, String sig) {
        try {
            new ProcessBuilder("kill", "-" + sig, Long.toString(processes[id].pid())).start().waitFor();
        } catch (IOException | InterruptedException e) {
            throw new IllegalStateException("cannot send SIG" + sig + " (needs a Unix-like OS)", e);
        }
    }

    private void cutBetween(List<Integer> a, List<Integer> b) {
        for (int x : a) {
            for (int y : b) {
                proxies[x][y].cut();
                proxies[y][x].cut();
            }
        }
    }

    private void healNetwork() {
        for (Proxy[] row : proxies) {
            for (Proxy p : row) {
                if (p != null) {
                    p.heal();
                }
            }
        }
    }

    private void healAll() throws IOException {
        healNetwork();
        for (int i = 1; i <= nodeCount; i++) {
            if (!processes[i].isAlive()) {
                startNode(i);
            }
            resume(i);
        }
    }

    private void nemesis() throws IOException, InterruptedException {
        int roll = random.nextInt(6);
        List<Integer> ids = new ArrayList<>();
        for (int i = 1; i <= nodeCount; i++) {
            ids.add(i);
        }
        switch (roll) {
            case 0 -> {
                int leader = findLeader();
                if (leader > 0) {
                    List<Integer> rest = new ArrayList<>(ids);
                    rest.remove(Integer.valueOf(leader));
                    healNetwork();
                    cutBetween(List.of(leader), rest);
                    log("nemesis: isolate the leader, n%d", leader);
                }
            }
            case 1 -> {
                Collections.shuffle(ids, random);
                int cut = 1 + random.nextInt(nodeCount - 1);
                healNetwork();
                cutBetween(ids.subList(0, cut), ids.subList(cut, ids.size()));
                log("nemesis: partition %s | %s", ids.subList(0, cut), ids.subList(cut, ids.size()));
            }
            case 2 -> {
                int victim = 1 + random.nextInt(nodeCount);
                kill(victim);
                log("nemesis: kill -9 n%d", victim);
                Thread.sleep(500 + random.nextInt(2000));
                startNode(victim);
                log("nemesis: restart n%d", victim);
            }
            case 3 -> {
                int leader = findLeader();
                int victim = leader > 0 ? leader : 1 + random.nextInt(nodeCount);
                pause(victim);
                log("nemesis: SIGSTOP n%d%s", victim, victim == leader ? " (the leader)" : "");
                Thread.sleep(1000 + random.nextInt(3000));
                resume(victim);
                log("nemesis: SIGCONT n%d", victim);
            }
            default -> {
                healNetwork();
                log("nemesis: heal the network");
            }
        }
    }

    private int findLeader() {
        for (int i = 1; i <= nodeCount; i++) {
            if (paused[i] || !processes[i].isAlive()) {
                continue;
            }
            try (RespClient c = new RespClient("127.0.0.1", clientPorts[i], 500)) {
                RespClient.Reply r = c.call("RAFT", "STATUS");
                if (r.text() != null && r.text().contains("role:leader")) {
                    return i;
                }
            } catch (IOException e) {
                // down or unreachable
            }
        }
        return 0;
    }

    private void waitForLeader(long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            int leader = findLeader();
            if (leader > 0) {
                log("cluster up, leader n%d", leader);
                return;
            }
            Thread.sleep(200);
        }
        throw new IllegalStateException("no leader after " + timeoutMs + " ms; logs in " + work);
    }

    // ---- clients ----

    private void clientLoop(int id, Random rnd) {
        int target = 1 + rnd.nextInt(nodeCount);
        RespClient conn = null;
        while (clientsRunning) {
            String key = "k" + rnd.nextInt(KEYS);
            int roll = rnd.nextInt(10);
            Op op = roll < 5 ? Op.get(key)
                    : roll < 8 ? Op.put(key, Long.toString(values.incrementAndGet()))
                    : Op.swap(key, Long.toString(values.incrementAndGet()));
            long called = now();
            int redirects = 0;
            while (true) {
                try {
                    if (conn == null) {
                        conn = new RespClient("127.0.0.1", clientPorts[target], CLIENT_TIMEOUT_MS);
                    }
                    RespClient.Reply r = switch (op.kind()) {
                        case GET -> conn.call("GET", key);
                        case PUT -> conn.call("SET", key, op.value());
                        default -> conn.call("SET", key, op.value(), "GET");
                    };
                    if (r.isError()) {
                        String msg = r.text();
                        if (msg.startsWith("MOVED") && redirects++ < 5) {
                            target = portToNode(Integer.parseInt(msg.substring(msg.lastIndexOf(':') + 1)));
                            conn.close();
                            conn = null;
                            continue;
                        }
                        if (msg.startsWith("TIMEOUT") && op.kind() != KvModel.Kind.GET) {
                            record(id, op, null, called, Operation.NEVER); // may or may not have happened
                            unknown.incrementAndGet();
                        } else {
                            failed.incrementAndGet(); // MOVED loop, CLUSTERDOWN, TRYAGAIN: definitely not applied
                        }
                        pauseBriefly(rnd);
                        if (msg.startsWith("CLUSTERDOWN")) {
                            target = 1 + rnd.nextInt(nodeCount);
                            conn.close();
                            conn = null;
                        }
                        break;
                    }
                    Reply reply = op.kind() == KvModel.Kind.PUT ? Reply.OK : Reply.value(r.text());
                    record(id, op, reply, called, now());
                    ok.incrementAndGet();
                    break;
                } catch (IOException e) {
                    // Timed out or the node died: the operation's fate is unknown.
                    if (conn != null) {
                        conn.close();
                        conn = null;
                    }
                    if (op.kind() != KvModel.Kind.GET) {
                        record(id, op, null, called, Operation.NEVER);
                    }
                    unknown.incrementAndGet();
                    target = 1 + rnd.nextInt(nodeCount);
                    pauseBriefly(rnd);
                    break;
                }
            }
        }
        if (conn != null) {
            conn.close();
        }
    }

    private int portToNode(int port) {
        for (int i = 1; i <= nodeCount; i++) {
            if (clientPorts[i] == port) {
                return i;
            }
        }
        return 1;
    }

    private void record(int client, Op op, Reply reply, long called, long returned) {
        history.add(new Operation<>(opIds.getAndIncrement(), client, op, reply, called, returned));
    }

    private static void pauseBriefly(Random rnd) {
        try {
            Thread.sleep(20 + rnd.nextInt(80));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // ---- verdict ----

    private int verdict() {
        List<Operation<Op, Reply>> ops;
        synchronized (history) {
            ops = List.copyOf(history);
        }
        log("%d operations: %d ok, %d failed (did not happen), %d unknown (may have happened)",
                ok.get() + failed.get() + unknown.get(), ok.get(), failed.get(), unknown.get());
        long t = System.nanoTime();
        Result r = Linearizability.check(KvModel.INSTANCE, ops, 50_000_000);
        log("linearizability check of %d operations: %s (%.1f s, %d states)", ops.size(), r.verdict(),
                (System.nanoTime() - t) / 1e9, r.statesExplored());
        if (r.ok()) {
            log("PASS: the history is linearizable. Logs: %s", work);
            return 0;
        }
        log("FAIL: %s", r.explanation());
        if (r.culprit() != null) {
            log("  %s   <- this one", describe(r.culprit()));
            r.concurrent().stream().limit(12).forEach(op -> log("  %s", describe(op)));
        }
        log("node logs: %s", work);
        return 1;
    }

    @SuppressWarnings("unchecked")
    private static String describe(Operation<?, ?> o) {
        var op = (Operation<Op, Reply>) o;
        String when = op.unknown() ? String.format(Locale.ROOT, "[%7.3f s, ?      ]", op.called() / 1e9)
                : String.format(Locale.ROOT, "[%7.3f s, %7.3f s]", op.called() / 1e9, op.returned() / 1e9);
        return when + "  client " + op.client() + "  " + KvModel.INSTANCE.describe(op.input(), op.output());
    }

    // ---- helpers ----

    private long now() {
        return System.nanoTime() - start;
    }

    private void log(String format, Object... args) {
        System.out.printf(Locale.ROOT, "%7.1f s  %s%n", (System.nanoTime() - start) / 1e9, String.format(Locale.ROOT, format, args));
    }

    private static String javaBinary() {
        return Path.of(System.getProperty("java.home"), "bin", "java").toString();
    }

    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }
}
