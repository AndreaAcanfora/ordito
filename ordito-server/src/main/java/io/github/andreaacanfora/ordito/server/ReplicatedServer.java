package io.github.andreaacanfora.ordito.server;

import io.github.andreaacanfora.loomkv.command.Broker;
import io.github.andreaacanfora.loomkv.command.CommandSpec;
import io.github.andreaacanfora.loomkv.command.Commands;
import io.github.andreaacanfora.loomkv.command.Context;
import io.github.andreaacanfora.loomkv.resp.RespValue;
import io.github.andreaacanfora.loomkv.store.Bytes;
import io.github.andreaacanfora.loomkv.store.Database;
import io.github.andreaacanfora.ordito.raft.Config;
import io.github.andreaacanfora.ordito.raft.Entry;
import io.github.andreaacanfora.ordito.raft.Message;
import io.github.andreaacanfora.ordito.raft.RaftNode;
import io.github.andreaacanfora.ordito.raft.RaftNode.Role;
import io.github.andreaacanfora.ordito.raft.ReadState;
import io.github.andreaacanfora.ordito.raft.Ready;
import io.github.andreaacanfora.ordito.raft.Rng;
import io.github.andreaacanfora.ordito.raft.Unsafe;
import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * One node of a replicated loomkv. Clients speak RESP to any node; the leader runs every write through
 * the Raft log and every read through ReadIndex, so the cluster behaves like a single loomkv that keeps
 * working while a majority of nodes is up. Followers answer with a {@code MOVED} redirect to the
 * leader, which {@code redis-cli -c} follows by itself.
 *
 * <p>One virtual thread owns the {@link RaftNode} and the database: it takes events from a queue
 * (ticks, messages, client requests), then persists, sends and applies what Raft asks for. Everything
 * that queued up meanwhile is handled before the next fsync, so concurrent writes share one fsync.
 *
 * <p>Commands are the unmodified loomkv ones. To make them deterministic on every node, each log entry
 * carries the leader's timestamp, and the state machine's clock is that timestamp: {@code SET k v EX 10}
 * expires at the same instant on every replica. While keys with a TTL exist, the leader appends empty
 * "clock" entries so they expire even when nobody writes.
 */
public final class ReplicatedServer implements Closeable {

    /**
     * @param peerVia where to reach some peers instead of their advertised address (for tests that put
     *     a proxy in between)
     */
    public record Options(int id, List<Member> cluster, Path dir, String bind, int tickMs,
            Map<Integer, InetSocketAddress> peerVia, boolean unsafeStaleReads) {

        public Options {
            cluster = List.copyOf(cluster);
            peerVia = Map.copyOf(peerVia);
            if (cluster.stream().noneMatch(m -> m.id() == id)) {
                throw new IllegalArgumentException("node " + id + " is not in the cluster");
            }
        }

        public Member self() {
            return cluster.stream().filter(m -> m.id() == id).findFirst().orElseThrow();
        }
    }

    /** What client threads may read about Raft without going through the loop. */
    public record Status(Role role, long term, int leader, long commit, long applied, long lastIndex) {}

    static final int ELECTION_TICKS = 15;
    static final int HEARTBEAT_TICKS = 3;
    static final long CLOCK_ENTRY_EVERY_MS = 100;
    static final long REQUEST_TIMEOUT_MS = 5000;

    private final Options options;
    private final BlockingQueue<Runnable> inbox = new LinkedBlockingQueue<>();
    private final ReplicatedClock clock = new ReplicatedClock();
    private final Database db = new Database(clock);
    private final Commands commands = new Commands();
    private final Broker broker = new LocalBroker();
    private DiskStorage storage;
    private Transport transport;
    private RaftNode raft;
    private ServerSocket clients;
    private volatile boolean running;
    private volatile Status status = new Status(Role.FOLLOWER, 0, RaftNode.NONE, 0, 0, 0);
    private final List<Thread> threads = new ArrayList<>();
    private Thread loopThread; // never interrupted: an interrupt during a write closes a FileChannel
    private final Set<ClientConnection> connections = java.util.concurrent.ConcurrentHashMap.newKeySet();

    // Owned by the raft loop.
    private final Map<Long, PendingWrite> pendingWrites = new HashMap<>();
    private final Map<Long, PendingRead> pendingReads = new HashMap<>();
    private final Set<Bytes> keysWithTtl = new HashSet<>();
    private long nextReadId = 1;
    private long lastTimestamp;
    private long lastProposalAt;
    private long applied;
    private Role lastRole = Role.FOLLOWER;
    private long replayed;
    private long ticks;

    private record PendingWrite(long term, byte[] data, CompletableFuture<RespValue> reply) {}

    private record PendingRead(List<byte[]> command, CompletableFuture<RespValue> reply) {}

    public ReplicatedServer(Options options) {
        this.options = options;
    }

    public ReplicatedServer start() throws IOException {
        storage = new DiskStorage(options.dir());
        DiskStorage.Loaded loaded = storage.loaded();
        if (loaded.truncatedBytes() > 0) {
            System.err.printf("ordito: node %d: cut %d bytes of an incomplete write off the log%n", options.id(),
                    loaded.truncatedBytes());
        }
        List<Integer> ids = options.cluster().stream().map(Member::id).toList();
        Config config = Config.of(options.id(), ids).withTicks(ELECTION_TICKS, HEARTBEAT_TICKS);
        if (options.unsafeStaleReads()) {
            // The classic broken "lease read": answer reads locally, and keep believing in a leadership
            // that a partition has already ended. For showing what the checker catches; never in real use.
            config = config.withUnsafe(Set.of(Unsafe.STALE_READS)).withCheckQuorum(false);
        }
        raft = new RaftNode(config, loaded.hardState(), loaded.entries(), new Rng(System.nanoTime() ^ options.id()));

        Map<Integer, InetSocketAddress> peers = new HashMap<>();
        for (Member m : options.cluster()) {
            if (m.id() != options.id()) {
                peers.put(m.id(), options.peerVia().getOrDefault(m.id(), new InetSocketAddress(m.host(), m.raftPort())));
            }
        }
        InetAddress bind = InetAddress.getByName(options.bind());
        Member self = options.self();
        transport = new Transport(new InetSocketAddress(bind, self.raftPort()), peers,
                message -> inbox.add(() -> raft.step(message)));
        running = true;
        processReady(); // re-applies what was committed before a restart
        replayed = applied;

        clients = new ServerSocket();
        clients.setReuseAddress(true);
        clients.bind(new InetSocketAddress(bind, self.clientPort()), 1024);
        loopThread = Thread.ofVirtual().name("raft-loop").start(this::loop);
        threads.add(Thread.ofVirtual().name("raft-ticker").start(this::ticker));
        threads.add(Thread.ofVirtual().name("client-accept").start(this::acceptClients));
        return this;
    }

    // ---- the raft loop ----

    private void loop() {
        List<Runnable> batch = new ArrayList<>();
        while (running) {
            try {
                Runnable first = inbox.poll(100, TimeUnit.MILLISECONDS);
                if (first == null) {
                    continue;
                }
                batch.add(first);
                inbox.drainTo(batch, 10_000);
                for (Runnable r : batch) {
                    r.run();
                }
                batch.clear();
                processReady();
            } catch (InterruptedException e) {
                return;
            } catch (UncheckedIOException e) {
                if (!running) {
                    return; // shutting down: the storage is being closed under us
                }
                // The disk failed: continuing could break Raft's promises. Stop the node.
                System.err.println("ordito: node " + options.id() + ": disk error, stopping: " + e.getCause().getMessage());
                running = false;
                Runtime.getRuntime().halt(70);
            }
        }
    }

    private void ticker() {
        while (running) {
            try {
                Thread.sleep(options.tickMs());
            } catch (InterruptedException e) {
                return;
            }
            inbox.add(this::tick);
        }
    }

    private void tick() {
        raft.tick();
        if (++ticks % 50 == 0) {
            try {
                storage.flushCommit();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        if (raft.role() == Role.LEADER && !keysWithTtl.isEmpty()
                && System.currentTimeMillis() - lastProposalAt >= CLOCK_ENTRY_EVERY_MS) {
            propose(List.of(), null); // advances the replicated clock so TTLs fire
        }
    }

    private void processReady() {
        Ready r = raft.ready();
        if (r.isEmpty()) {
            publishStatus();
            return;
        }
        // A leader's AppendEntries can go out while it writes the same entries to its own disk (Raft
        // thesis, 10.2.1): it only counts its own copy when computing the commit index, and that happens
        // in a later step, after this fsync. Votes and replies must wait for the disk.
        List<Message> early = new ArrayList<>();
        List<Message> late = new ArrayList<>();
        for (Message m : r.messages()) {
            (m instanceof Message.AppendEntries ? early : late).add(m);
        }
        transport.send(early);
        try {
            storage.save(r.hardState(), r.persistFrom(), r.entries());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        transport.send(late);
        for (Entry e : r.committed()) {
            apply(e);
        }
        for (ReadState rs : r.reads()) {
            PendingRead read = pendingReads.remove(rs.requestId());
            if (read != null) {
                read.reply().complete(execute(read.command()));
            }
        }
        Role role = raft.role();
        if (lastRole == Role.LEADER && role != Role.LEADER) {
            for (PendingRead read : pendingReads.values()) {
                read.reply().complete(RespValue.error("TRYAGAIN leadership changed during the read"));
            }
            pendingReads.clear();
        }
        lastRole = role;
        publishStatus();
    }

    private void publishStatus() {
        status = new Status(raft.role(), raft.term(), raft.leader(), raft.commitIndex(), applied, raft.lastIndex());
    }

    private void apply(Entry e) {
        applied = e.index();
        if (e.isNoop()) {
            return;
        }
        ByteBuffer data = ByteBuffer.wrap(e.data());
        clock.advanceTo(data.getLong());
        List<byte[]> args = decodeArgs(data);
        RespValue reply = RespValue.OK;
        if (!args.isEmpty()) {
            reply = execute(args);
            trackTtl(args);
        } else {
            keysWithTtl.removeIf(key -> db.locked(() -> db.ttlMillis(key)) < 0);
        }
        db.expireDue(1000);
        PendingWrite waiting = pendingWrites.remove(e.index());
        if (waiting != null) {
            boolean ours = waiting.term() == e.term() && Arrays.equals(waiting.data(), e.data());
            if (waiting.reply() != null) {
                waiting.reply().complete(ours ? reply
                        : RespValue.error("TRYAGAIN leadership changed before the write committed; it was not applied"));
            }
        }
    }

    private RespValue execute(List<byte[]> args) {
        return commands.execute(new Context(db, broker, args), null);
    }

    /** Remembers keys that may have a TTL, so the leader knows to keep the clock moving. */
    private void trackTtl(List<byte[]> args) {
        for (int i = 1; i <= 2 && i < args.size(); i++) {
            Bytes key = new Bytes(args.get(i));
            if (db.locked(() -> db.ttlMillis(key)) >= 0) {
                keysWithTtl.add(key);
            }
        }
    }

    private void propose(List<byte[]> args, CompletableFuture<RespValue> reply) {
        if (raft.role() != Role.LEADER) {
            if (reply != null) {
                reply.complete(redirect(status));
            }
            return;
        }
        long now = System.currentTimeMillis();
        lastTimestamp = Math.max(Math.max(now, lastTimestamp), clock.millis());
        lastProposalAt = now;
        byte[] data = encode(lastTimestamp, args);
        long index = raft.propose(data);
        pendingWrites.put(index, new PendingWrite(raft.term(), data, reply));
    }

    private void read(List<byte[]> args, CompletableFuture<RespValue> reply) {
        if (raft.role() != Role.LEADER) {
            reply.complete(redirect(status));
            return;
        }
        long id = nextReadId++;
        pendingReads.put(id, new PendingRead(args, reply));
        raft.requestRead(id);
    }

    // ---- entry encoding: timestamp, then the command's arguments ----

    static byte[] encode(long timestamp, List<byte[]> args) {
        int size = 8 + 4;
        for (byte[] a : args) {
            size += 4 + a.length;
        }
        ByteBuffer b = ByteBuffer.allocate(size);
        b.putLong(timestamp).putInt(args.size());
        for (byte[] a : args) {
            b.putInt(a.length).put(a);
        }
        return b.array();
    }

    private static List<byte[]> decodeArgs(ByteBuffer b) {
        int n = b.getInt();
        List<byte[]> args = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            byte[] a = new byte[b.getInt()];
            b.get(a);
            args.add(a);
        }
        return args;
    }

    // ---- clients ----

    private void acceptClients() {
        while (running) {
            try {
                Socket socket = clients.accept();
                socket.setTcpNoDelay(true);
                ClientConnection c = new ClientConnection(this, socket);
                connections.add(c);
                Thread.ofVirtual().name("client").start(() -> {
                    try {
                        c.run();
                    } finally {
                        connections.remove(c);
                    }
                });
            } catch (IOException e) {
                if (running) {
                    System.err.println("ordito: accept failed: " + e.getMessage());
                }
            }
        }
    }

    /** Runs one command for a client: locally, through the log, or as a linearizable read. */
    RespValue handle(List<byte[]> args) {
        CommandSpec spec = commands.lookup(new String(args.getFirst(), java.nio.charset.StandardCharsets.UTF_8));
        if (spec == null || !spec.arityOk(args.size()) || spec.name().equals("publish")) {
            return execute(args); // the error loomkv would give, or a local command
        }
        Status s = status;
        if (s.role() != Role.LEADER) {
            return redirect(s);
        }
        CompletableFuture<RespValue> reply = new CompletableFuture<>();
        if (spec.write()) {
            inbox.add(() -> propose(args, reply));
        } else {
            inbox.add(() -> read(args, reply));
        }
        try {
            return reply.get(REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (java.util.concurrent.TimeoutException e) {
            return RespValue.error(spec.write()
                    ? "TIMEOUT no majority answered in time; the write may or may not have been applied"
                    : "TIMEOUT no majority answered in time");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return RespValue.error("ERR interrupted");
        } catch (java.util.concurrent.ExecutionException e) {
            return RespValue.error("ERR " + e.getCause().getMessage());
        }
    }

    /** Commands answered by this node alone, without consensus. */
    RespValue local(List<byte[]> args) {
        return execute(args);
    }

    RespValue redirect(Status s) {
        if (s.leader() == RaftNode.NONE || s.leader() == options.id()) {
            return RespValue.error("CLUSTERDOWN no leader right now, try again shortly");
        }
        Member leader = options.cluster().stream().filter(m -> m.id() == s.leader()).findFirst().orElseThrow();
        return RespValue.error("MOVED 0 " + leader.clientAddress());
    }

    String statusText() {
        Status s = status;
        return String.join("\r\n",
                "id:" + options.id(),
                "role:" + s.role().name().toLowerCase(java.util.Locale.ROOT),
                "term:" + s.term(),
                "leader:" + s.leader(),
                "commit_index:" + s.commit(),
                "last_applied:" + s.applied(),
                "last_index:" + s.lastIndex(),
                "members:" + options.cluster().size(),
                "");
    }

    public Status status() {
        return status;
    }

    public int clientPort() {
        return clients.getLocalPort();
    }

    public long replayedEntries() {
        return replayed;
    }

    public Options options() {
        return options;
    }

    private final class LocalBroker implements Broker {
        @Override
        public int publish(Bytes channel, byte[] message) {
            return 0; // pub/sub is not replicated
        }

        @Override
        public String info() {
            return String.join("\r\n",
                    "# Server",
                    "redis_version:7.4.0",
                    "ordito_version:" + Main.VERSION,
                    "java_version:" + Runtime.version(),
                    "process_id:" + ProcessHandle.current().pid(),
                    "tcp_port:" + options.self().clientPort(),
                    "",
                    "# Raft",
                    statusText(),
                    "# Keyspace",
                    "db0:keys=" + db.locked(db::size),
                    "");
        }
    }

    @Override
    public void close() throws IOException {
        running = false;
        if (clients != null) {
            clients.close();
        }
        for (ClientConnection c : connections) {
            c.close();
        }
        if (transport != null) {
            transport.close();
        }
        threads.forEach(Thread::interrupt);
        if (loopThread != null) {
            threads.add(loopThread); // it sees running == false within one poll and exits
        }
        for (Thread t : threads) {
            try {
                t.join(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        if (storage != null) {
            storage.close();
        }
    }

    /** The state machine's clock: the timestamp of the last applied entry. Never goes back. */
    static final class ReplicatedClock extends Clock {
        private volatile long millis;

        void advanceTo(long t) {
            if (t > millis) {
                millis = t;
            }
        }

        @Override
        public long millis() {
            return millis;
        }

        @Override
        public Instant instant() {
            return Instant.ofEpochMilli(millis);
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }
    }
}
