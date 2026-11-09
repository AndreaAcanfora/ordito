package io.github.andreaacanfora.ordito.raft;

import io.github.andreaacanfora.ordito.raft.Message.AppendEntries;
import io.github.andreaacanfora.ordito.raft.Message.AppendResponse;
import io.github.andreaacanfora.ordito.raft.Message.RequestVote;
import io.github.andreaacanfora.ordito.raft.Message.VoteResponse;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;

/**
 * One Raft node as a pure state machine. It never blocks, reads a clock, touches a disk or opens a
 * socket: the host feeds it {@link #tick()}s, incoming {@link Message}s and client requests, then
 * collects what to persist, send and apply with {@link #ready()}. That is what makes it testable with a
 * deterministic simulator, where one seed reproduces a whole run, failures included.
 *
 * <p>Implements leader election with pre-vote and check-quorum, log replication with fast backtracking,
 * the commit rule for the leader's own term, and linearizable reads with ReadIndex.
 *
 * <p>Not thread safe: the host calls it from one thread.
 */
public final class RaftNode {

    /** "Nobody": no vote cast, no known leader. */
    public static final int NONE = 0;

    public enum Role { FOLLOWER, PRE_CANDIDATE, CANDIDATE, LEADER }

    private final Config config;
    private final Rng rng;
    private final int id;
    private final int[] members;
    private final int quorum;
    private final RaftLog log;

    private Role role = Role.FOLLOWER;
    private long term;
    private int votedFor;
    private int leader = NONE;
    private long commit;
    private long applied;

    private int electionElapsed;
    private int heartbeatElapsed;
    private int electionTimeout;

    // Candidate state, indexed like members: 0 = no answer, 1 = granted, -1 = rejected.
    private final int[] votes;

    // Leader state, indexed like members.
    private final long[] next;
    private final long[] match;
    private final boolean[] active;
    private final long[] ackedSeq;
    // After a rejection a follower is probed: one AppendEntries at a time, at most one per heartbeat,
    // until it accepts one. Without this, duplicated rejections each trigger a resend, and a lossy,
    // duplicating network turns that into a storm (the simulator found this).
    private final boolean[] probing;
    private final boolean[] probeSent;
    private long readSeq;
    private final Deque<PendingRead> pendingReads = new ArrayDeque<>();
    private final List<Long> readsAwaitingCommit = new ArrayList<>();

    // Output collected for the next Ready.
    private final List<Message> outbox = new ArrayList<>();
    private final List<ReadState> readyReads = new ArrayList<>();
    private long persistFrom;
    private boolean broadcastPending;
    private HardState saved;

    private record PendingRead(long requestId, long index, long seq) {}

    /** A brand-new node with an empty log. */
    public RaftNode(Config config, Rng rng) {
        this(config, HardState.INITIAL, List.of(), rng);
    }

    /** A node restarting from what it had on disk. The state machine is rebuilt by re-applying the log. */
    public RaftNode(Config config, HardState state, List<Entry> entries, Rng rng) {
        this.config = config;
        this.rng = rng;
        this.id = config.id();
        this.members = config.members().stream().mapToInt(Integer::intValue).toArray();
        this.quorum = members.length / 2 + 1;
        this.log = new RaftLog(entries);
        this.term = state.term();
        this.votedFor = state.votedFor();
        this.commit = Math.min(state.commit(), log.lastIndex());
        this.saved = new HardState(term, persistedVote(), commit);
        this.votes = new int[members.length];
        this.next = new long[members.length];
        this.match = new long[members.length];
        this.active = new boolean[members.length];
        this.ackedSeq = new long[members.length];
        this.probing = new boolean[members.length];
        this.probeSent = new boolean[members.length];
        resetElectionTimeout();
    }

    // ---- inputs ----

    /** Advances logical time by one tick. */
    public void tick() {
        if (role == Role.LEADER) {
            electionElapsed++;
            if (electionElapsed >= config.electionTicks()) {
                electionElapsed = 0;
                if (config.checkQuorum() && !quorumActive()) {
                    becomeFollower(term, NONE); // cut off from the majority: stop accepting writes
                    return;
                }
                Arrays.fill(active, false);
            }
            if (++heartbeatElapsed >= config.heartbeatTicks()) {
                heartbeatElapsed = 0;
                broadcastPending = true;
                Arrays.fill(probeSent, false);
            }
            return;
        }
        if (++electionElapsed >= electionTimeout) {
            campaign(config.preVote());
        }
    }

    /** Handles a message from another node. */
    public void step(Message m) {
        if (m.to() != id || indexOf(m.from()) < 0) {
            return;
        }
        if (m.term() > term) {
            if (m instanceof RequestVote && inLease()) {
                return; // we hear from a live leader: an election now would only disrupt it
            }
            boolean preVoteTraffic = m instanceof RequestVote rv && rv.preVote()
                    || m instanceof VoteResponse vr && vr.preVote() && vr.granted();
            if (!preVoteTraffic) {
                becomeFollower(m.term(), m instanceof AppendEntries ? m.from() : NONE);
            }
        } else if (m.term() < term) {
            // A stale leader or pre-candidate: answer so it learns the newer term.
            if (m instanceof AppendEntries ae) {
                send(new AppendResponse(id, ae.from(), term, false, 0, ae.readSeq()));
            } else if (m instanceof RequestVote rv && rv.preVote()) {
                send(new VoteResponse(id, rv.from(), term, false, true));
            }
            return;
        }
        switch (m) {
            case RequestVote rv -> handleVoteRequest(rv);
            case VoteResponse vr -> handleVoteResponse(vr);
            case AppendEntries ae -> handleAppend(ae);
            case AppendResponse ar -> handleAppendResponse(ar);
        }
    }

    /**
     * Appends a client command to the log if this node is the leader. Returns its index, or -1 if this
     * node is not the leader. The command is applied once {@link Ready#committed()} hands it back.
     */
    public long propose(byte[] data) {
        if (role != Role.LEADER) {
            return -1;
        }
        long index = log.lastIndex() + 1;
        appendToLog(new Entry(index, term, data.clone()));
        maybeCommit();
        broadcastPending = true;
        return index;
    }

    /**
     * Starts a linearizable read on the leader. Returns false if this node is not the leader. Once a
     * majority confirms the leadership, a {@link ReadState} with this id appears in {@link Ready#reads()}.
     */
    public boolean requestRead(long requestId) {
        if (role != Role.LEADER) {
            return false;
        }
        if (config.has(Unsafe.STALE_READS)) {
            readyReads.add(new ReadState(requestId, commit));
            return true;
        }
        if (log.term(commit) != term) {
            // Until an entry of this term commits, the leader does not know the true commit index.
            readsAwaitingCommit.add(requestId);
            return true;
        }
        startRead(requestId);
        return true;
    }

    // ---- output ----

    /** Collects and clears everything to persist, send, apply and answer. */
    public Ready ready() {
        if (broadcastPending && role == Role.LEADER) {
            broadcastPending = false;
            for (int i = 0; i < members.length; i++) {
                if (members[i] != id) {
                    sendAppend(i);
                }
            }
        }
        broadcastPending = false;
        HardState hs = new HardState(term, persistedVote(), commit);
        HardState changed = hs.equals(saved) ? null : hs;
        saved = hs;
        List<Entry> entries = persistFrom > 0 ? log.slice(persistFrom, log.lastIndex()) : List.of();
        long from = persistFrom;
        persistFrom = 0;
        List<Entry> committed = log.slice(applied + 1, commit);
        applied = commit;
        Ready ready = new Ready(changed, from, entries, List.copyOf(outbox), committed, List.copyOf(readyReads));
        outbox.clear();
        readyReads.clear();
        return ready;
    }

    // ---- elections ----

    private void campaign(boolean pre) {
        if (pre) {
            role = Role.PRE_CANDIDATE;
            leader = NONE;
        } else {
            role = Role.CANDIDATE;
            term++;
            votedFor = id;
            leader = NONE;
        }
        electionElapsed = 0;
        resetElectionTimeout();
        Arrays.fill(votes, 0);
        votes[indexOf(id)] = 1;
        if (granted() >= quorum) { // single-node cluster
            if (pre) {
                campaign(false);
            } else {
                becomeLeader();
            }
            return;
        }
        long voteTerm = pre ? term + 1 : term;
        for (int member : members) {
            if (member != id) {
                send(new RequestVote(id, member, voteTerm, log.lastIndex(), log.lastTerm(), pre));
            }
        }
    }

    private void handleVoteRequest(RequestVote rv) {
        boolean canVote = votedFor == rv.from()
                || (votedFor == NONE && leader == NONE)
                || (rv.preVote() && rv.term() > term);
        boolean upToDate = config.has(Unsafe.VOTE_FOR_STALE_LOG)
                || rv.lastLogTerm() > log.lastTerm()
                || (rv.lastLogTerm() == log.lastTerm() && rv.lastLogIndex() >= log.lastIndex());
        if (canVote && upToDate) {
            send(new VoteResponse(id, rv.from(), rv.term(), true, rv.preVote()));
            if (!rv.preVote()) {
                votedFor = rv.from();
                electionElapsed = 0;
            }
        } else {
            send(new VoteResponse(id, rv.from(), term, false, rv.preVote()));
        }
    }

    private void handleVoteResponse(VoteResponse vr) {
        boolean expected = (role == Role.PRE_CANDIDATE && vr.preVote()) || (role == Role.CANDIDATE && !vr.preVote());
        if (!expected) {
            return;
        }
        votes[indexOf(vr.from())] = vr.granted() ? 1 : -1;
        if (granted() >= quorum) {
            if (role == Role.PRE_CANDIDATE) {
                campaign(false);
            } else {
                becomeLeader();
            }
        } else if (rejected() >= quorum) {
            becomeFollower(term, NONE);
        }
    }

    private void becomeFollower(long newTerm, int newLeader) {
        if (newTerm != term) {
            term = newTerm;
            votedFor = NONE;
        }
        role = Role.FOLLOWER;
        leader = newLeader;
        electionElapsed = 0;
        resetElectionTimeout();
        pendingReads.clear();
        readsAwaitingCommit.clear();
    }

    private void becomeLeader() {
        role = Role.LEADER;
        leader = id;
        electionElapsed = 0;
        heartbeatElapsed = 0;
        Arrays.fill(next, log.lastIndex() + 1);
        Arrays.fill(match, 0);
        Arrays.fill(active, false);
        Arrays.fill(ackedSeq, 0);
        Arrays.fill(probing, true); // until each follower confirms where its log ends
        Arrays.fill(probeSent, false);
        readSeq = 0;
        pendingReads.clear();
        readsAwaitingCommit.clear();
        // A no-op in the new term: committing it commits everything before it (section 5.4.2).
        appendToLog(new Entry(log.lastIndex() + 1, term, new byte[0]));
        maybeCommit();
        broadcastPending = true;
    }

    // ---- replication ----

    private void sendAppend(int peer) {
        if (probing[peer]) {
            if (probeSent[peer]) {
                return; // one probe in flight per heartbeat is enough
            }
            probeSent[peer] = true;
        }
        long prev = next[peer] - 1;
        long last = Math.min(log.lastIndex(), prev + config.maxBatch());
        List<Entry> entries = log.slice(prev + 1, last);
        send(new AppendEntries(id, members[peer], term, prev, log.term(prev), entries, commit, readSeq));
        if (!probing[peer]) {
            next[peer] = last + 1; // optimistic, pipelined: a rejection moves it back
        }
    }

    private void handleAppend(AppendEntries ae) {
        if (role != Role.FOLLOWER) {
            if (role == Role.LEADER) {
                return; // two leaders in one term: only possible with an Unsafe bug turned on
            }
            becomeFollower(term, ae.from());
        }
        leader = ae.from();
        electionElapsed = 0;
        if (ae.prevLogIndex() < commit) {
            // Already have a committed prefix past this point; tell the leader where we are.
            send(new AppendResponse(id, ae.from(), term, true, commit, ae.readSeq()));
            return;
        }
        if (!log.matches(ae.prevLogIndex(), ae.prevLogTerm())) {
            long hint = ae.prevLogIndex() > log.lastIndex()
                    ? log.lastIndex() + 1
                    : log.firstIndexOfTerm(ae.prevLogIndex(), commit);
            send(new AppendResponse(id, ae.from(), term, false, hint, ae.readSeq()));
            return;
        }
        for (Entry e : ae.entries()) {
            long existing = log.term(e.index());
            if (existing == e.term()) {
                continue; // already have it (a duplicate or reordered message)
            }
            if (existing != -1) {
                if (e.index() <= commit) {
                    throw new IllegalStateException("would overwrite committed entry " + e.index());
                }
                log.truncateFrom(e.index());
                markDirty(e.index());
            }
            appendToLog(e);
        }
        long lastNew = ae.prevLogIndex() + ae.entries().size();
        if (ae.leaderCommit() > commit) {
            commit = Math.min(ae.leaderCommit(), lastNew);
        }
        send(new AppendResponse(id, ae.from(), term, true, lastNew, ae.readSeq()));
    }

    private void handleAppendResponse(AppendResponse ar) {
        if (role != Role.LEADER) {
            return;
        }
        int p = indexOf(ar.from());
        active[p] = true;
        if (ar.readSeq() > ackedSeq[p]) {
            ackedSeq[p] = ar.readSeq();
            confirmReads();
        }
        if (ar.success()) {
            if (ar.index() > match[p]) {
                match[p] = ar.index();
                maybeCommit();
            }
            if (next[p] <= match[p]) {
                next[p] = match[p] + 1;
            }
            probing[p] = false;
            if (next[p] <= log.lastIndex()) {
                sendAppend(p); // still behind: keep the entries flowing
            }
        } else {
            // Back off to the follower's hint, by at least one entry, never below what it has matched.
            long previous = next[p];
            next[p] = Math.max(match[p] + 1, Math.min(next[p] - 1, ar.index()));
            if (next[p] < previous || !probing[p]) {
                probing[p] = true;
                sendAppend(p);
            }
        }
    }

    private void maybeCommit() {
        long[] indexes = new long[members.length];
        for (int i = 0; i < members.length; i++) {
            indexes[i] = members[i] == id ? log.lastIndex() : match[i];
        }
        Arrays.sort(indexes);
        long candidate = indexes[members.length - quorum]; // the highest index a majority has
        if (candidate <= commit) {
            return;
        }
        // Only entries of the current term are committed by counting replicas (section 5.4.2).
        if (log.term(candidate) != term && !config.has(Unsafe.COMMIT_PREVIOUS_TERM)) {
            return;
        }
        commit = candidate;
        broadcastPending = true;
        if (!readsAwaitingCommit.isEmpty() && log.term(commit) == term) {
            List<Long> waiting = List.copyOf(readsAwaitingCommit);
            readsAwaitingCommit.clear();
            waiting.forEach(this::startRead);
        }
    }

    // ---- reads ----

    private void startRead(long requestId) {
        readSeq++;
        pendingReads.add(new PendingRead(requestId, commit, readSeq));
        broadcastPending = true; // the next heartbeat round carries readSeq
        confirmReads(); // a single-node cluster confirms at once
    }

    private void confirmReads() {
        while (!pendingReads.isEmpty()) {
            PendingRead read = pendingReads.peek();
            int acks = 0;
            for (int i = 0; i < members.length; i++) {
                if (members[i] == id || ackedSeq[i] >= read.seq()) {
                    acks++;
                }
            }
            if (acks < quorum) {
                return;
            }
            pendingReads.poll();
            readyReads.add(new ReadState(read.requestId(), read.index()));
        }
    }

    // ---- helpers ----

    private void appendToLog(Entry e) {
        log.append(e);
        markDirty(e.index());
    }

    private void markDirty(long index) {
        if (persistFrom == 0 || index < persistFrom) {
            persistFrom = index;
        }
    }

    private void send(Message m) {
        outbox.add(m);
    }

    private boolean inLease() {
        return config.checkQuorum() && leader != NONE && electionElapsed < config.electionTicks();
    }

    private boolean quorumActive() {
        int count = 0;
        for (int i = 0; i < members.length; i++) {
            if (members[i] == id || active[i]) {
                count++;
            }
        }
        return count >= quorum;
    }

    private int granted() {
        int n = 0;
        for (int v : votes) {
            n += v == 1 ? 1 : 0;
        }
        return n;
    }

    private int rejected() {
        int n = 0;
        for (int v : votes) {
            n += v == -1 ? 1 : 0;
        }
        return n;
    }

    private int persistedVote() {
        return config.has(Unsafe.FORGET_VOTE) ? NONE : votedFor;
    }

    private void resetElectionTimeout() {
        electionTimeout = config.electionTicks() + rng.nextInt(config.electionTicks());
    }

    private int indexOf(int member) {
        for (int i = 0; i < members.length; i++) {
            if (members[i] == member) {
                return i;
            }
        }
        return -1;
    }

    // ---- inspection, for hosts, the simulator and the playground ----

    public int id() {
        return id;
    }

    public Role role() {
        return role;
    }

    public long term() {
        return term;
    }

    public int leader() {
        return leader;
    }

    public int votedFor() {
        return votedFor;
    }

    public long commitIndex() {
        return commit;
    }

    public long lastIndex() {
        return log.lastIndex();
    }

    public long lastTerm() {
        return log.lastTerm();
    }

    /** The term of the entry at an index, 0 for index 0, -1 if absent. */
    public long termAt(long index) {
        return log.term(index);
    }

    public Entry entry(long index) {
        return log.get(index);
    }

    public List<Entry> entries(long from, long to) {
        return log.slice(from, to);
    }

    /** Leader only: the highest index known replicated on each member, in {@link Config#members()} order. */
    public long[] matchIndexes() {
        long[] out = match.clone();
        out[indexOf(id)] = log.lastIndex();
        return out;
    }

    public Config config() {
        return config;
    }
}
