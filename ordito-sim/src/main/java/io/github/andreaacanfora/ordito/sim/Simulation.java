package io.github.andreaacanfora.ordito.sim;

import io.github.andreaacanfora.ordito.check.KvModel;
import io.github.andreaacanfora.ordito.check.KvModel.Kind;
import io.github.andreaacanfora.ordito.check.KvModel.Op;
import io.github.andreaacanfora.ordito.check.KvModel.Reply;
import io.github.andreaacanfora.ordito.check.Linearizability;
import io.github.andreaacanfora.ordito.check.Operation;
import io.github.andreaacanfora.ordito.check.Result;
import io.github.andreaacanfora.ordito.raft.Config;
import io.github.andreaacanfora.ordito.raft.Entry;
import io.github.andreaacanfora.ordito.raft.HardState;
import io.github.andreaacanfora.ordito.raft.Message;
import io.github.andreaacanfora.ordito.raft.RaftNode;
import io.github.andreaacanfora.ordito.raft.RaftNode.Role;
import io.github.andreaacanfora.ordito.raft.ReadState;
import io.github.andreaacanfora.ordito.raft.Ready;
import io.github.andreaacanfora.ordito.raft.Rng;
import io.github.andreaacanfora.ordito.raft.Unsafe;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Consumer;

/**
 * A whole cluster in one thread: nodes, network, disks and clients, all driven by one {@link Rng}.
 * There is no real time and no real concurrency, so a seed replays a run exactly, failures included.
 *
 * <p>While it runs it checks Raft's safety properties after every step (one leader per term, no two
 * nodes applying different commands at one index, every new leader holding every committed entry). At
 * the end it heals everything, checks the cluster recovers, and checks the clients' history for
 * linearizability.
 */
public final class Simulation {

    public static final int TICK_MS = 10;
    public static final int ELECTION_TICKS = 15;
    public static final int HEARTBEAT_TICKS = 3;
    static final long CLIENT_TIMEOUT_MS = 1000;
    static final long RECOVERY_MS = 3000;
    static final long LYING_FSYNC_INTERVAL_MS = 300;
    static final long MAX_MESSAGES = 3_000_000;

    // ---- events ----

    sealed interface Event {}

    record Tick(int node, int generation) implements Event {}

    record Deliver(Message message) implements Event {}

    record ToNode(int node, int client, long opId, Op op) implements Event {}

    record ToClient(int client, long opId, Outcome outcome) implements Event {}

    record ClientTimeout(int client, long opId) implements Event {}

    record ClientNext(int client) implements Event {}

    record Nemesis() implements Event {}

    record Resume(int node, int generation) implements Event {}

    record Heal() implements Event {}

    record Restart(int node) implements Event {}

    /** A node's answer to a client. */
    sealed interface Outcome {}

    /** Not the leader; try {@code leader} (or anyone, if NONE). The operation did not happen. */
    record Redirect(int leader) implements Outcome {}

    /** The operation definitely did not happen (its entry was overwritten, or leadership was lost). */
    record Rejected() implements Outcome {}

    record Done(Reply reply) implements Outcome {}

    private record Scheduled(long time, long seq, Event event) {}

    // ---- state ----

    /** A simulated server: Raft, its disk, and the key-value state machine. */
    public final class Node {
        final int id;
        RaftNode raft;
        boolean up = true;
        boolean paused;
        int generation;
        int tickMs = TICK_MS;
        final Disk disk = new Disk();
        TreeMap<String, String> kv = new TreeMap<>();
        long applied;
        final TreeMap<Long, Command> pendingWrites = new TreeMap<>();
        final TreeMap<Long, Command> pendingReads = new TreeMap<>();
        long nextReadId = 1;
        Role lastRole = Role.FOLLOWER;
        final List<Scheduled> deferred = new ArrayList<>();
        long lastSync;

        Node(int id) {
            this.id = id;
        }

        public int id() {
            return id;
        }

        public RaftNode raft() {
            return raft;
        }

        public boolean up() {
            return up;
        }

        public boolean paused() {
            return paused;
        }

        public Map<String, String> kv() {
            return kv;
        }

        public long applied() {
            return applied;
        }

        public int tickMs() {
            return tickMs;
        }
    }

    /** Durable state, and with {@link Bug#LYING_FSYNC} the part of it that only looked durable. */
    static final class Disk {
        HardState hardState = HardState.INITIAL;
        List<Entry> log = new ArrayList<>();
        HardState syncedHardState = HardState.INITIAL;
        List<Entry> syncedLog = new ArrayList<>();

        void sync() {
            syncedHardState = hardState;
            syncedLog = new ArrayList<>(log);
        }

        void loseUnsynced() {
            hardState = syncedHardState;
            log = new ArrayList<>(syncedLog);
        }
    }

    private final class Client {
        final int id;
        long opId;
        Op op;
        long called;
        boolean busy;
        int target;
        final Map<String, String> lastSeen = new HashMap<>();

        Client(int id) {
            this.id = id;
        }
    }

    /** What went wrong, if anything. */
    public enum Failure { NONE, ELECTION_SAFETY, STATE_MACHINE_SAFETY, LEADER_COMPLETENESS, PANIC, LIVENESS, LINEARIZABILITY }

    private final SimConfig config;
    private final Environment environment;

    /**
     * The conditions of one run. With swarm testing the seed picks them, so some runs have tiny batches
     * and a lossy network, others fast links and frequent faults.
     */
    public record Environment(int maxBatch, double dropRate, double duplicateRate, int maxLatencyMs, int faultGapMs) {

        static Environment pick(Rng rng) {
            int[] batches = {1, 2, 3, 8, 64};
            double[] drops = {0, 0.01, 0.05, 0.2};
            double[] dups = {0, 0.01, 0.1};
            int[] latencies = {2, 15, 60};
            int[] gaps = {300, 1500, 4000};
            return new Environment(batches[rng.nextInt(batches.length)], drops[rng.nextInt(drops.length)],
                    dups[rng.nextInt(dups.length)], latencies[rng.nextInt(latencies.length)], gaps[rng.nextInt(gaps.length)]);
        }

        @Override
        public String toString() {
            return "batch " + maxBatch + ", " + Math.round(dropRate * 100) + "% loss, " + Math.round(duplicateRate * 100)
                    + "% duplicates, latency up to " + maxLatencyMs + " ms, a fault every ~" + faultGapMs + " ms";
        }
    }
    private final Rng netRng;
    private final Rng nemesisRng;
    private final Rng clientRng;
    private final Rng nodeRng;
    private final List<Integer> ids = new ArrayList<>();
    private final Node[] nodes;
    private final Client[] clients;
    private final boolean[][] blocked;
    private final PriorityQueue<Scheduled> queue = new PriorityQueue<>(
            (a, b) -> a.time != b.time ? Long.compare(a.time, b.time) : Long.compare(a.seq, b.seq));
    private long seq;
    private long now;
    private boolean faultsActive;
    private boolean clientsActive = true;
    private long healedAt = -1;
    private long firstWriteAfterHeal = -1;

    // invariant bookkeeping
    private final Map<Long, Integer> leaderOfTerm = new HashMap<>();
    private final List<Entry> committed = new ArrayList<>(); // index i+1 -> the entry everyone must apply there
    private final List<Integer> committedBy = new ArrayList<>();
    private final List<Long> commitTerm = new ArrayList<>(); // the term of the leader that committed index i+1

    private final List<Operation<Op, Reply>> history = new ArrayList<>();
    private long nextOpId = 1;
    private Failure failure = Failure.NONE;
    private String failureMessage = "";
    private Result linearizability;

    // stats
    private long messagesSent;
    private long messagesDropped;
    private long elections;
    private long faultCount;
    private long completed;
    private long unknown;

    private final Deque<String> recent = new ArrayDeque<>();
    private static final int RECENT_LINES = 60;
    private Consumer<String> log;

    public Simulation(SimConfig config) {
        this.config = config;
        Rng root = new Rng(config.seed());
        this.netRng = root.fork();
        this.nemesisRng = root.fork();
        this.clientRng = root.fork();
        this.nodeRng = root.fork();
        this.environment = config.swarm() ? Environment.pick(root.fork())
                : new Environment(64, config.dropRate(), config.duplicateRate(), config.maxLatencyMs(), 1500);
        for (int i = 1; i <= config.nodes(); i++) {
            ids.add(i);
        }
        nodes = new Node[config.nodes() + 1];
        blocked = new boolean[config.nodes() + 1][config.nodes() + 1];
        for (int id : ids) {
            nodes[id] = new Node(id);
            startNode(nodes[id]);
        }
        clients = new Client[config.clients()];
        for (int c = 0; c < clients.length; c++) {
            clients[c] = new Client(c + 1);
            clients[c].target = 1 + clientRng.nextInt(config.nodes());
            schedule(clientRng.nextInt(200), new ClientNext(c));
        }
        faultsActive = config.faults();
        if (faultsActive) {
            schedule(500 + nemesisRng.nextInt(1000), new Nemesis());
        }
        schedule(config.durationMs(), new Heal());
    }

    /** Sends every log line (elections, faults, client results) to {@code sink} as it happens. */
    public Simulation onLog(Consumer<String> sink) {
        this.log = sink;
        return this;
    }

    // ---- running ----

    /** Runs to the end: the fault phase, recovery, and the final checks. */
    public Simulation run() {
        long end = config.durationMs() + RECOVERY_MS;
        runUntil(end);
        clientsActive = false;
        runUntil(end + CLIENT_TIMEOUT_MS + 100); // let in-flight operations finish or time out
        if (failure == Failure.NONE && config.clients() > 0 && firstWriteAfterHeal < 0) {
            fail(Failure.LIVENESS, "no write completed in the " + RECOVERY_MS + " ms after the network healed");
        }
        if (failure == Failure.NONE) {
            checkHistory();
        }
        return this;
    }

    /** Processes events up to {@code time}, or until a safety property breaks. */
    public void runUntil(long time) {
        while (failure == Failure.NONE && !queue.isEmpty() && queue.peek().time <= time) {
            Scheduled s = queue.poll();
            now = s.time;
            handle(s);
        }
        if (failure == Failure.NONE) {
            now = Math.max(now, time);
        }
    }

    /** Checks the history recorded so far for linearizability. */
    public Result checkHistory() {
        linearizability = Linearizability.check(KvModel.INSTANCE, history, 20_000_000);
        if (linearizability.verdict() == Result.Verdict.NOT_LINEARIZABLE && failure == Failure.NONE) {
            fail(Failure.LINEARIZABILITY, linearizability.explanation());
        }
        return linearizability;
    }

    private void handle(Scheduled s) {
        switch (s.event) {
            case Tick t -> onTick(t);
            case Deliver d -> {
                Node n = nodes[d.message().to()];
                if (deferIfPaused(n, s)) {
                    return;
                }
                if (!n.up || blocked[d.message().from()][d.message().to()]) {
                    messagesDropped++;
                    return;
                }
                guarded(n, () -> n.raft.step(d.message()));
            }
            case ToNode r -> {
                Node n = nodes[r.node()];
                if (deferIfPaused(n, s)) {
                    return;
                }
                if (n.up) {
                    guarded(n, () -> clientRequest(n, r));
                }
            }
            case ToClient r -> onClientReply(r);
            case ClientTimeout t -> onClientTimeout(t);
            case ClientNext c -> nextOperation(clients[c.client()]);
            case Nemesis ignored -> nemesis();
            case Resume r -> {
                Node n = nodes[r.node()];
                if (n.up && n.paused && n.generation == r.generation()) {
                    resume(n);
                }
            }
            case Heal ignored -> healAll();
            case Restart r -> {
                if (faultsActive) {
                    restart(nodes[r.node()]);
                }
            }
            case RetryLater r -> {
                Client c = clients[r.client()];
                if (c.busy && c.opId == r.opId()) {
                    sendToNode(c);
                }
            }
        }
    }

    private boolean deferIfPaused(Node n, Scheduled s) {
        if (n.up && n.paused) {
            n.deferred.add(s);
            return true;
        }
        return false;
    }

    private void onTick(Tick t) {
        Node n = nodes[t.node()];
        if (!n.up || n.generation != t.generation() || n.paused) {
            return; // a stale timer from before a crash, or frozen
        }
        if (config.has(Bug.LYING_FSYNC) && now - n.lastSync >= LYING_FSYNC_INTERVAL_MS) {
            n.disk.sync();
            n.lastSync = now;
        }
        guarded(n, n.raft::tick);
        schedule(n.tickMs, new Tick(n.id, n.generation));
    }

    /** Calls into Raft, then does what its Ready asks. A thrown exception is a panic: a failed run. */
    private void guarded(Node n, Runnable input) {
        try {
            input.run();
            process(n);
        } catch (RuntimeException e) {
            fail(Failure.PANIC, "node " + n.id + " panicked: " + e.getMessage());
        }
    }

    private void process(Node n) {
        Ready r = n.raft.ready();
        // 1. persist
        if (r.hardState() != null) {
            n.disk.hardState = r.hardState();
        }
        if (r.persistFrom() > 0) {
            List<Entry> disk = n.disk.log;
            disk.subList((int) r.persistFrom() - 1, disk.size()).clear();
            disk.addAll(r.entries());
        }
        if (!config.has(Bug.LYING_FSYNC)) {
            n.disk.syncedHardState = n.disk.hardState; // fsync: what was written is durable
            n.disk.syncedLog = n.disk.log;
        }
        // 2. send
        for (Message m : r.messages()) {
            send(m);
        }
        // 3. apply
        for (Entry e : r.committed()) {
            apply(n, e);
            if (failure != Failure.NONE) {
                return;
            }
        }
        // 4. reads
        for (ReadState rs : r.reads()) {
            Command c = n.pendingReads.remove(rs.requestId());
            if (c != null) {
                String value = n.kv.get(c.op().key());
                replyTo(c, new Done(Reply.value(value)));
            }
        }
        if (n.raft.role() == Role.LEADER) {
            while (commitTerm.size() < n.raft.commitIndex()) {
                commitTerm.add(n.raft.term());
            }
        }
        observeRole(n);
    }

    private void observeRole(Node n) {
        Role role = n.raft.role();
        long term = n.raft.term();
        if (role == Role.LEADER) {
            Integer previous = leaderOfTerm.putIfAbsent(term, n.id);
            if (previous != null && previous != n.id) {
                fail(Failure.ELECTION_SAFETY, "nodes " + previous + " and " + n.id + " are both leader of term " + term);
                return;
            }
            if (n.lastRole != Role.LEADER) {
                elections++;
                note("n" + n.id + " becomes leader of term " + term);
                // Leader completeness: an entry committed in an earlier term is in every later leader's log.
                // (A leader of an older term can still win late, with delayed votes; it owes nothing to
                // entries committed after its term.)
                for (int i = 0; i < committed.size() && i < commitTerm.size(); i++) {
                    Entry e = committed.get(i);
                    if (commitTerm.get(i) < term && n.raft.termAt(e.index()) != e.term()) {
                        fail(Failure.LEADER_COMPLETENESS, "n" + n.id + " became leader of term " + term
                                + " without entry " + e.index() + ", committed in term " + commitTerm.get(i));
                        return;
                    }
                }
            }
        } else if (n.lastRole == Role.LEADER) {
            note("n" + n.id + " steps down (term " + term + ")");
            // Reads in flight never happened; the clients may retry them.
            for (Command c : n.pendingReads.values()) {
                replyTo(c, new Rejected());
            }
            n.pendingReads.clear();
        }
        n.lastRole = role;
    }

    private void apply(Node n, Entry e) {
        n.applied = e.index();
        int i = (int) e.index() - 1;
        if (i < committed.size()) {
            Entry agreed = committed.get(i);
            if (!agreed.equals(e)) {
                fail(Failure.STATE_MACHINE_SAFETY, "n" + n.id + " applied " + describe(e) + " at index " + e.index()
                        + " but n" + committedBy.get(i) + " applied " + describe(agreed));
                return;
            }
        } else {
            committed.add(e);
            committedBy.add(n.id);
        }
        if (e.isNoop()) {
            return;
        }
        Command c = Command.decode(e.data());
        Reply reply = execute(n.kv, c.op());
        Command waiting = n.pendingWrites.remove(e.index());
        if (waiting != null) {
            boolean ours = waiting.opId() == c.opId();
            replyTo(waiting, ours ? new Done(reply) : new Rejected());
        }
    }

    private static Reply execute(TreeMap<String, String> kv, Op op) {
        String current = kv.get(op.key());
        return switch (op.kind()) {
            case GET -> Reply.value(current);
            case PUT -> {
                kv.put(op.key(), op.value());
                yield Reply.OK;
            }
            case CAS -> {
                if (Objects.equals(current, op.expected())) {
                    kv.put(op.key(), op.value());
                    yield Reply.OK;
                }
                yield Reply.FAILED;
            }
            case SWAP -> {
                kv.put(op.key(), op.value());
                yield Reply.value(current);
            }
        };
    }

    private String describe(Entry e) {
        if (e.isNoop()) {
            return "a no-op of term " + e.term();
        }
        Command c = Command.decode(e.data());
        return KvModel.INSTANCE.describe(c.op(), null).replace(" -> ?", "") + " of term " + e.term();
    }

    // ---- network ----

    private void send(Message m) {
        if (++messagesSent > MAX_MESSAGES) {
            fail(Failure.LIVENESS, "message storm: more than " + MAX_MESSAGES + " messages in one run");
            return;
        }
        if (netRng.chance(environment.dropRate())) {
            messagesDropped++;
            return;
        }
        schedule(1 + netRng.nextInt(environment.maxLatencyMs()), new Deliver(m));
        if (netRng.chance(environment.duplicateRate())) {
            schedule(1 + netRng.nextInt(environment.maxLatencyMs() * 2), new Deliver(m));
        }
    }

    private int clientLatency() {
        return 1 + netRng.nextInt(5);
    }

    // ---- clients ----

    private void nextOperation(Client c) {
        if (!clientsActive || c.busy) {
            return;
        }
        String key = "k" + clientRng.nextInt(config.keys());
        String value = Long.toString(nextOpId);
        int roll = clientRng.nextInt(10);
        Op op;
        if (roll < 5) {
            op = Op.get(key);
        } else if (roll < 8) {
            op = Op.put(key, value);
        } else {
            op = Op.cas(key, c.lastSeen.get(key), value);
        }
        c.op = op;
        c.opId = nextOpId++;
        c.called = now;
        c.busy = true;
        sendToNode(c);
        schedule(CLIENT_TIMEOUT_MS, new ClientTimeout(c.id - 1, c.opId));
    }

    private void sendToNode(Client c) {
        schedule(clientLatency(), new ToNode(c.target, c.id - 1, c.opId, c.op));
    }

    private void clientRequest(Node n, ToNode r) {
        Command c = new Command(r.client(), r.opId(), r.op());
        if (n.raft.role() != Role.LEADER) {
            replyTo(c, new Redirect(n.raft.leader()));
            return;
        }
        if (r.op().kind() == Kind.GET) {
            long id = n.nextReadId++;
            n.pendingReads.put(id, c);
            n.raft.requestRead(id);
        } else {
            long index = n.raft.propose(c.encode());
            n.pendingWrites.put(index, c);
        }
    }

    private void replyTo(Command c, Outcome outcome) {
        schedule(clientLatency(), new ToClient(c.client(), c.opId(), outcome));
    }

    private void onClientReply(ToClient r) {
        Client c = clients[r.client()];
        if (!c.busy || c.opId != r.opId()) {
            return; // the answer to an operation that already timed out
        }
        switch (r.outcome()) {
            case Redirect redirect -> {
                c.target = redirect.leader() != RaftNode.NONE ? redirect.leader() : 1 + clientRng.nextInt(config.nodes());
                schedule(5 + clientRng.nextInt(20), new RetryLater(c.id - 1, c.opId));
            }
            case Rejected ignored -> schedule(5 + clientRng.nextInt(20), new RetryLater(c.id - 1, c.opId));
            case Done done -> {
                Reply reply = done.reply();
                history.add(new Operation<>(history.size(), c.id, c.op, reply, c.called, now));
                completed++;
                if (c.op.kind() == Kind.GET || c.op.kind() == Kind.SWAP) {
                    c.lastSeen.put(c.op.key(), reply.value());
                } else if (reply.ok()) {
                    c.lastSeen.put(c.op.key(), c.op.value());
                }
                if (c.op.kind() != Kind.GET && healedAt >= 0 && c.called >= healedAt && firstWriteAfterHeal < 0) {
                    firstWriteAfterHeal = now;
                }
                if (log != null) {
                    emit("client " + c.id + " " + KvModel.INSTANCE.describe(c.op, reply));
                }
                c.busy = false;
                schedule(clientRng.nextInt(20), new ClientNext(c.id - 1));
            }
        }
    }

    /** A client retrying after a definite "no": the operation did not happen, so retrying is safe. */
    record RetryLater(int client, long opId) implements Event {}

    private void onClientTimeout(ClientTimeout t) {
        Client c = clients[t.client()];
        if (!c.busy || c.opId != t.opId()) {
            return;
        }
        if (c.op.kind() != Kind.GET) {
            // It may still take effect at any later point, or never: the checker considers both.
            history.add(new Operation<>(history.size(), c.id, c.op, null, c.called, Operation.NEVER));
        }
        unknown++;
        c.busy = false;
        c.target = 1 + clientRng.nextInt(config.nodes());
        schedule(clientRng.nextInt(20), new ClientNext(c.id - 1));
    }

    // ---- faults ----

    private void nemesis() {
        if (!faultsActive) {
            return;
        }
        faultCount++;
        int roll = nemesisRng.nextInt(100);
        if (roll < 20) {
            partitionRandomly();
        } else if (roll < 32) {
            int leader = currentLeader();
            if (leader != RaftNode.NONE) {
                isolate(leader);
            } else {
                partitionRandomly();
            }
        } else if (roll < 40) {
            bridge();
        } else if (roll < 55) {
            heal();
        } else if (roll < 60) {
            Node n = randomNode(true);
            if (n != null) {
                crash(n);
            }
        } else if (roll < 67) {
            // kill -9 and an immediate restart by the supervisor: memory is lost, the disk is not
            Node n = randomNode(true);
            if (n != null) {
                crash(n);
                schedule(nemesisRng.nextInt(50), new Restart(n.id));
            }
        } else if (roll < 72) {
            // the leader dies: most interesting histories start here
            int leader = currentLeader();
            if (leader != RaftNode.NONE) {
                crash(nodes[leader]);
                schedule(nemesisRng.nextInt(400), new Restart(leader));
            }
        } else if (roll < 77) {
            Node n = randomDownNode();
            if (n != null) {
                restart(n);
            } else {
                heal();
            }
        } else if (roll < 87) {
            Node n = randomNode(true);
            if (n != null && !n.paused) {
                pause(n, 200 + nemesisRng.nextInt(1500));
            }
        } else {
            Node n = nodes[1 + nemesisRng.nextInt(config.nodes())];
            n.tickMs = 6 + nemesisRng.nextInt(10); // a fast or slow clock
            note("n" + n.id + " clock now ticks every " + n.tickMs + " ms");
        }
        schedule(environment.faultGapMs() / 5 + nemesisRng.nextInt(environment.faultGapMs()), new Nemesis());
    }

    private void partitionRandomly() {
        List<Integer> shuffled = new ArrayList<>(ids);
        for (int i = shuffled.size() - 1; i > 0; i--) {
            int j = nemesisRng.nextInt(i + 1);
            Integer tmp = shuffled.get(i);
            shuffled.set(i, shuffled.get(j));
            shuffled.set(j, tmp);
        }
        int cut = 1 + nemesisRng.nextInt(Math.max(1, ids.size() - 1));
        List<Integer> a = shuffled.subList(0, cut);
        List<Integer> b = shuffled.subList(cut, shuffled.size());
        partition(List.of(List.copyOf(a), List.copyOf(b)));
    }

    /** Cuts the network into groups that cannot talk to each other. */
    public void partition(List<List<Integer>> groups) {
        int[] group = new int[nodes.length];
        for (int g = 0; g < groups.size(); g++) {
            for (int id : groups.get(g)) {
                group[id] = g + 1;
            }
        }
        for (int a : ids) {
            for (int b : ids) {
                blocked[a][b] = group[a] != group[b];
            }
        }
        StringBuilder text = new StringBuilder("partition");
        for (List<Integer> g : groups) {
            text.append(' ').append(g);
        }
        note(text.toString());
    }

    public void isolate(int id) {
        List<Integer> rest = new ArrayList<>(ids);
        rest.remove(Integer.valueOf(id));
        partition(List.of(List.of(id), rest));
    }

    /** Two halves that cannot talk, joined by one node that can talk to both. */
    private void bridge() {
        if (ids.size() < 3) {
            heal();
            return;
        }
        int middle = 1 + nemesisRng.nextInt(ids.size());
        for (int a : ids) {
            for (int b : ids) {
                boolean sameSide = (a < middle) == (b < middle);
                blocked[a][b] = a != middle && b != middle && !sameSide;
            }
        }
        note("bridge: n" + middle + " is the only link between the two sides");
    }

    public void heal() {
        for (boolean[] row : blocked) {
            Arrays.fill(row, false);
        }
        note("network healed");
    }

    public void crash(Node n) {
        if (!n.up) {
            return;
        }
        n.up = false;
        n.paused = false;
        n.raft = null;
        n.generation++;
        n.kv = new TreeMap<>();
        n.applied = 0;
        n.pendingWrites.clear();
        n.pendingReads.clear();
        n.deferred.clear();
        n.lastRole = Role.FOLLOWER;
        n.disk.loseUnsynced();
        note("n" + n.id + " crashed");
    }

    public void restart(Node n) {
        if (n.up) {
            return;
        }
        startNode(n);
        guarded(n, () -> {}); // re-applies the committed log
        note("n" + n.id + " restarted (term " + n.raft.term() + ", " + n.raft.lastIndex() + " entries on disk)");
    }

    private void startNode(Node n) {
        EnumSet<Unsafe> unsafe = EnumSet.noneOf(Unsafe.class);
        for (Bug bug : config.bugs()) {
            if (bug.unsafe() != null) {
                unsafe.add(bug.unsafe());
            }
        }
        Config base = Config.of(n.id, ids);
        Config cfg = new Config(n.id, ids, ELECTION_TICKS, HEARTBEAT_TICKS, environment.maxBatch(), base.preVote(),
                base.checkQuorum(), unsafe);
        n.raft = new RaftNode(cfg, n.disk.hardState, List.copyOf(n.disk.log), nodeRng.fork());
        n.up = true;
        n.lastSync = now;
        schedule(1 + nodeRng.nextInt(n.tickMs), new Tick(n.id, n.generation));
    }

    public void pause(Node n, long ms) {
        if (!n.up || n.paused) {
            return;
        }
        n.paused = true;
        note("n" + n.id + " paused for " + ms + " ms");
        schedule(ms, new Resume(n.id, n.generation));
    }

    public void resume(Node n) {
        n.paused = false;
        note("n" + n.id + " resumed");
        // Everything that arrived while it was frozen is handled now, in arrival order.
        for (Scheduled s : n.deferred) {
            queue.add(new Scheduled(now, seq++, s.event));
        }
        n.deferred.clear();
        schedule(n.tickMs, new Tick(n.id, n.generation));
    }

    private void healAll() {
        faultsActive = false;
        heal();
        for (int id : ids) {
            Node n = nodes[id];
            n.tickMs = TICK_MS;
            if (!n.up) {
                restart(n);
            } else if (n.paused) {
                resume(n);
            }
        }
        healedAt = now;
        note("faults stopped: the cluster must now recover");
    }

    private int currentLeader() {
        int leader = RaftNode.NONE;
        long best = -1;
        for (int id : ids) {
            Node n = nodes[id];
            if (n.up && n.raft.role() == Role.LEADER && n.raft.term() > best) {
                best = n.raft.term();
                leader = id;
            }
        }
        return leader;
    }

    private Node randomNode(boolean mustBeUp) {
        List<Node> candidates = new ArrayList<>();
        for (int id : ids) {
            if (!mustBeUp || nodes[id].up) {
                candidates.add(nodes[id]);
            }
        }
        return candidates.isEmpty() ? null : candidates.get(nemesisRng.nextInt(candidates.size()));
    }

    private Node randomDownNode() {
        List<Node> down = new ArrayList<>();
        for (int id : ids) {
            if (!nodes[id].up) {
                down.add(nodes[id]);
            }
        }
        return down.isEmpty() ? null : down.get(nemesisRng.nextInt(down.size()));
    }

    // ---- helpers ----

    private void schedule(long delay, Event e) {
        queue.add(new Scheduled(now + delay, seq++, e));
    }

    private void fail(Failure kind, String message) {
        if (failure == Failure.NONE) {
            failure = kind;
            failureMessage = message;
            note("VIOLATION " + kind + ": " + message);
        }
    }

    private void note(String line) {
        emit(line);
    }

    private void emit(String line) {
        String stamped = String.format("%8d ms  %s", now, line);
        recent.addLast(stamped);
        if (recent.size() > RECENT_LINES) {
            recent.removeFirst();
        }
        if (log != null) {
            log.accept(stamped);
        }
    }

    // ---- results and inspection ----

    public SimConfig config() {
        return config;
    }

    public long now() {
        return now;
    }

    public Failure failure() {
        return failure;
    }

    public String failureMessage() {
        return failureMessage;
    }

    public boolean ok() {
        return failure == Failure.NONE;
    }

    public Result linearizability() {
        return linearizability;
    }

    public List<String> recentLog() {
        return List.copyOf(recent);
    }

    public List<Operation<Op, Reply>> history() {
        return List.copyOf(history);
    }

    public Node node(int id) {
        return nodes[id];
    }

    public List<Integer> nodeIds() {
        return List.copyOf(ids);
    }

    public boolean linkBlocked(int from, int to) {
        return blocked[from][to];
    }

    public long committedIndex() {
        return committed.size();
    }

    public Stats stats() {
        return new Stats(messagesSent, messagesDropped, elections, faultCount, completed, unknown, committed.size());
    }

    public record Stats(long messages, long dropped, long elections, long faults, long operations, long unknown,
            long committedEntries) {}

    /** Messages on the wire right now, for the playground. */
    public List<InFlight> inFlight() {
        List<InFlight> out = new ArrayList<>();
        for (Scheduled s : queue) {
            if (s.event instanceof Deliver d) {
                out.add(new InFlight(d.message(), s.time));
            }
        }
        return out;
    }

    public record InFlight(Message message, long arrives) {}

    public Environment environment() {
        return environment;
    }

    public Set<Bug> bugs() {
        return config.bugs();
    }
}
