package io.github.andreaacanfora.ordito.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Command-line entry point. */
public final class Main {

    public static final String VERSION = versionFromManifest();

    private static final String USAGE = """
            ordito-server: loomkv replicated with Raft. Any Redis client works; writes survive node failures.

            Usage:
              java -jar ordito-server.jar --demo [--nodes 3]
                  start a whole cluster in this process, on ports 6381, 6382, ... (data in ./ordito-demo)

              java -jar ordito-server.jar --id <n> --cluster <spec> [options]
                  start one node of a cluster
                  --cluster <spec>         every node as id=host:clientPort:raftPort, comma separated,
                                           e.g. 1=127.0.0.1:6381:7381,2=127.0.0.1:6382:7382,3=127.0.0.1:6383:7383
                  --dir <path>             where this node keeps its log (default ./ordito-data/<id>)
                  --bind <address>         address to listen on (default 127.0.0.1)
                  --tick-ms <n>            Raft tick (default 20: heartbeats every 60 ms, elections after 300-600 ms)
                  --peer-via <id=host:port,...>  reach some peers through another address (a proxy)
                  --unsafe-stale-reads     serve reads without confirming leadership (a deliberate bug, for tests)

            Then: redis-cli -c -p 6381   (-c follows the MOVED redirect that followers send to the leader)
            """;

    private Main() {}

    public static void main(String[] args) throws IOException, InterruptedException {
        try {
            if (args.length == 0 || List.of(args).contains("--help") || List.of(args).contains("-h")) {
                System.out.print(USAGE);
                return;
            }
            if (List.of(args).contains("--version")) {
                System.out.println("ordito " + VERSION);
                return;
            }
            if (List.of(args).contains("--demo")) {
                demo(args);
                return;
            }
            ReplicatedServer server = new ReplicatedServer(parse(args)).start();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    server.close();
                } catch (IOException e) {
                    System.err.println("ordito: error while stopping: " + e.getMessage());
                }
            }));
            var o = server.options();
            System.out.printf("ordito %s: node %d of %d, clients on %s:%d, log in %s, %d committed entries replayed%n",
                    VERSION, o.id(), o.cluster().size(), o.bind(), server.clientPort(), o.dir(), server.replayedEntries());
            Thread.currentThread().join();
        } catch (IllegalArgumentException e) {
            System.err.println("error: " + e.getMessage());
            System.err.print(USAGE);
            System.exit(2);
        }
    }

    static ReplicatedServer.Options parse(String[] args) {
        Integer id = null;
        List<Member> cluster = null;
        Path dir = null;
        String bind = "127.0.0.1";
        int tickMs = 20;
        Map<Integer, InetSocketAddress> via = new HashMap<>();
        boolean staleReads = false;
        for (int i = 0; i < args.length; i++) {
            String a = args[i];
            if (a.equals("--unsafe-stale-reads")) {
                staleReads = true;
                continue;
            }
            if (i + 1 >= args.length) {
                throw new IllegalArgumentException(a + " needs a value");
            }
            String v = args[++i];
            switch (a) {
                case "--id" -> id = Integer.parseInt(v);
                case "--cluster" -> cluster = Member.parseCluster(v);
                case "--dir" -> dir = Path.of(v);
                case "--bind" -> bind = v;
                case "--tick-ms" -> tickMs = Integer.parseInt(v);
                case "--peer-via" -> {
                    for (String part : v.split(",")) {
                        String[] kv = part.split("=", 2);
                        String[] hp = kv[1].split(":");
                        via.put(Integer.parseInt(kv[0]), new InetSocketAddress(hp[0], Integer.parseInt(hp[1])));
                    }
                }
                default -> throw new IllegalArgumentException("unknown option " + a);
            }
        }
        if (id == null || cluster == null) {
            throw new IllegalArgumentException("--id and --cluster are required");
        }
        if (dir == null) {
            dir = Path.of("ordito-data", Integer.toString(id));
        }
        return new ReplicatedServer.Options(id, cluster, dir, bind, tickMs, via, staleReads);
    }

    private static void demo(String[] args) throws IOException, InterruptedException {
        int n = 3;
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].equals("--nodes")) {
                n = Integer.parseInt(args[i + 1]);
            }
        }
        List<Member> cluster = new ArrayList<>();
        for (int i = 1; i <= n; i++) {
            cluster.add(new Member(i, "127.0.0.1", 6380 + i, 7380 + i));
        }
        List<ReplicatedServer> servers = new ArrayList<>();
        for (Member m : cluster) {
            servers.add(new ReplicatedServer(new ReplicatedServer.Options(m.id(), cluster,
                    Path.of("ordito-demo", Integer.toString(m.id())), "127.0.0.1", 20, Map.of(), false)).start());
        }
        Runtime.getRuntime().addShutdownHook(new Thread(() -> servers.forEach(s -> {
            try {
                s.close();
            } catch (IOException ignored) {
                // stopping anyway
            }
        })));
        System.out.printf("ordito %s demo: %d nodes on ports %d-%d. Try: redis-cli -c -p 6381%n", VERSION, n, 6381, 6380 + n);
        while (true) {
            Thread.sleep(1000);
            for (ReplicatedServer s : servers) {
                if (s.status().role() == io.github.andreaacanfora.ordito.raft.RaftNode.Role.LEADER) {
                    System.out.printf("\rleader: node %d (port %d), term %d, %d entries committed   ",
                            s.options().id(), s.clientPort(), s.status().term(), s.status().commit());
                }
            }
        }
    }

    private static String versionFromManifest() {
        String v = Main.class.getPackage().getImplementationVersion();
        return v != null ? v : "dev";
    }
}
