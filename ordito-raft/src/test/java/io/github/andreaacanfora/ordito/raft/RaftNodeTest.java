package io.github.andreaacanfora.ordito.raft;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.andreaacanfora.ordito.raft.RaftNode.Role;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.Test;

class RaftNodeTest {

    /** A tiny synchronous cluster: messages are delivered at once unless a link is cut. */
    static final class Cluster {
        final Map<Integer, RaftNode> nodes = new HashMap<>();
        final Map<Integer, HardState> hardStates = new HashMap<>();
        final Map<Integer, List<Entry>> disks = new HashMap<>();
        final Map<Integer, List<byte[]>> applied = new HashMap<>();
        final Map<Integer, List<ReadState>> reads = new HashMap<>();
        final Set<Integer> down = new HashSet<>();
        final Set<Long> cut = new HashSet<>();
        final List<Integer> ids;
        final UnaryOperator<Config> tweak;
        final Rng rng = new Rng(7);

        Cluster(int n) {
            this(n, c -> c);
        }

        Cluster(int n, UnaryOperator<Config> tweak) {
            this.tweak = tweak;
            ids = new ArrayList<>();
            for (int i = 1; i <= n; i++) {
                ids.add(i);
            }
            for (int id : ids) {
                hardStates.put(id, HardState.INITIAL);
                disks.put(id, new ArrayList<>());
                start(id);
            }
        }

        void start(int id) {
            nodes.put(id, new RaftNode(tweak.apply(Config.of(id, ids)), hardStates.get(id),
                    List.copyOf(disks.get(id)), rng.fork()));
            applied.put(id, new ArrayList<>());
            reads.put(id, new ArrayList<>());
            down.remove(id);
        }

        void crash(int id) {
            down.add(id);
        }

        void isolate(int id) {
            for (int other : ids) {
                if (other != id) {
                    cut.add(link(id, other));
                    cut.add(link(other, id));
                }
            }
        }

        void heal() {
            cut.clear();
        }

        static long link(int from, int to) {
            return ((long) from << 32) | to;
        }

        void tick(int times) {
            for (int t = 0; t < times; t++) {
                for (int id : ids) {
                    if (!down.contains(id)) {
                        nodes.get(id).tick();
                    }
                }
                flush();
            }
        }

        void flush() {
            boolean progress = true;
            while (progress) {
                progress = false;
                for (int id : ids) {
                    if (down.contains(id)) {
                        continue;
                    }
                    Ready r = nodes.get(id).ready();
                    if (r.isEmpty()) {
                        continue;
                    }
                    progress = true;
                    if (r.hardState() != null) {
                        hardStates.put(id, r.hardState());
                    }
                    if (r.persistFrom() > 0) {
                        List<Entry> disk = disks.get(id);
                        disk.subList((int) r.persistFrom() - 1, disk.size()).clear();
                        disk.addAll(r.entries());
                    }
                    for (Message m : r.messages()) {
                        if (!down.contains(m.to()) && !cut.contains(link(m.from(), m.to()))) {
                            nodes.get(m.to()).step(m);
                        }
                    }
                    for (Entry e : r.committed()) {
                        if (!e.isNoop()) {
                            applied.get(id).add(e.data());
                        }
                    }
                    reads.get(id).addAll(r.reads());
                }
            }
        }

        int leader() {
            int found = RaftNode.NONE;
            for (int id : ids) {
                if (!down.contains(id) && nodes.get(id).role() == Role.LEADER) {
                    found = id;
                }
            }
            return found;
        }

        int electLeader() {
            for (int i = 0; i < 100 && leader() == RaftNode.NONE; i++) {
                tick(1);
            }
            int leader = leader();
            assertNotEquals(RaftNode.NONE, leader, "no leader elected");
            return leader;
        }
    }

    static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void electsExactlyOneLeader() {
        Cluster c = new Cluster(3);
        int leader = c.electLeader();
        long term = c.nodes.get(leader).term();
        for (int id : c.ids) {
            assertEquals(term, c.nodes.get(id).term());
            assertEquals(leader, c.nodes.get(id).leader());
        }
    }

    @Test
    void replicatesAndAppliesInOrder() {
        Cluster c = new Cluster(5);
        int leader = c.electLeader();
        c.nodes.get(leader).propose(b("a"));
        c.nodes.get(leader).propose(b("b"));
        c.flush();
        c.tick(3);
        for (int id : c.ids) {
            assertEquals(2, c.applied.get(id).size(), "node " + id);
            assertArrayEquals(b("a"), c.applied.get(id).get(0));
            assertArrayEquals(b("b"), c.applied.get(id).get(1));
        }
    }

    @Test
    void singleNodeCommitsAlone() {
        Cluster c = new Cluster(1);
        int leader = c.electLeader();
        assertEquals(2, c.nodes.get(leader).propose(b("x")));
        c.flush();
        assertEquals(1, c.applied.get(leader).size());
        assertTrue(c.nodes.get(leader).requestRead(9));
        c.flush();
        assertEquals(List.of(new ReadState(9, 2)), c.reads.get(leader));
    }

    @Test
    void followersRejectProposals() {
        Cluster c = new Cluster(3);
        int leader = c.electLeader();
        int follower = leader % 3 + 1;
        assertEquals(-1, c.nodes.get(follower).propose(b("x")));
        assertFalse(c.nodes.get(follower).requestRead(1));
    }

    @Test
    void writesNeedAMajority() {
        Cluster c = new Cluster(3);
        int leader = c.electLeader();
        c.isolate(leader);
        c.nodes.get(leader).propose(b("lost"));
        c.flush();
        assertEquals(0, c.applied.get(leader).size());
        assertEquals(1, c.nodes.get(leader).commitIndex()); // only the no-op
    }

    @Test
    void isolatedLeaderStepsDownAndMajorityMovesOn() {
        Cluster c = new Cluster(3);
        int old = c.electLeader();
        c.isolate(old);
        c.tick(40);
        assertNotEquals(Role.LEADER, c.nodes.get(old).role(), "check-quorum makes it step down");
        int fresh = c.leader();
        assertNotEquals(RaftNode.NONE, fresh);
        assertNotEquals(old, fresh);
    }

    @Test
    void preVoteStopsARejoiningNodeFromDisruptingTheCluster() {
        Cluster c = new Cluster(3);
        int leader = c.electLeader();
        long term = c.nodes.get(leader).term();
        int follower = leader % 3 + 1;
        c.isolate(follower);
        c.tick(100); // the isolated follower keeps timing out, but only pre-votes
        assertEquals(term, c.nodes.get(follower).term());
        c.heal();
        c.tick(20);
        assertEquals(leader, c.leader());
        assertEquals(term, c.nodes.get(leader).term());
    }

    @Test
    void withoutPreVoteARejoiningNodeBumpsTheTerm() {
        Cluster c = new Cluster(3, cfg -> cfg.withPreVote(false));
        int leader = c.electLeader();
        long term = c.nodes.get(leader).term();
        int follower = leader % 3 + 1;
        c.isolate(follower);
        c.tick(100);
        assertTrue(c.nodes.get(follower).term() > term + 1);
    }

    @Test
    void readsNeedTheLeaderToBeConfirmedByAMajority() {
        Cluster c = new Cluster(3);
        int leader = c.electLeader();
        c.isolate(leader);
        assertTrue(c.nodes.get(leader).requestRead(1));
        c.tick(3);
        assertTrue(c.reads.get(leader).isEmpty(), "a cut-off leader must not serve reads");

        Cluster ok = new Cluster(3);
        int l = ok.electLeader();
        ok.tick(2);
        assertTrue(ok.nodes.get(l).requestRead(1));
        ok.flush();
        assertEquals(1, ok.reads.get(l).size());
        assertEquals(ok.nodes.get(l).commitIndex(), ok.reads.get(l).getFirst().index());
    }

    @Test
    void staleReadsBugServesReadsWithoutConfirmation() {
        Cluster c = new Cluster(3, cfg -> cfg.withUnsafe(Set.of(Unsafe.STALE_READS)));
        int leader = c.electLeader();
        c.isolate(leader);
        c.nodes.get(leader).requestRead(1);
        c.flush();
        assertEquals(1, c.reads.get(leader).size());
    }

    @Test
    void divergentFollowerLogIsRepaired() {
        Cluster c = new Cluster(3);
        int leader = c.electLeader();
        c.isolate(leader);
        for (int i = 0; i < 5; i++) {
            c.nodes.get(leader).propose(b("orphan" + i)); // never committed
        }
        c.flush();
        c.tick(40);
        int fresh = c.leader();
        c.nodes.get(fresh).propose(b("kept"));
        c.flush();
        c.heal();
        c.tick(20);
        for (int id : c.ids) {
            List<byte[]> a = c.applied.get(id);
            assertEquals(1, a.size(), "node " + id);
            assertArrayEquals(b("kept"), a.getFirst());
            assertEquals(c.nodes.get(fresh).lastIndex(), c.nodes.get(id).lastIndex());
        }
    }

    @Test
    void restartKeepsTheLogAndReappliesIt() {
        Cluster c = new Cluster(3);
        int leader = c.electLeader();
        c.nodes.get(leader).propose(b("durable"));
        c.flush();
        c.tick(3);
        int follower = leader % 3 + 1;
        c.crash(follower);
        c.start(follower);
        c.flush();
        assertEquals(1, c.applied.get(follower).size(), "committed entries are re-applied on restart");
        assertArrayEquals(b("durable"), c.applied.get(follower).getFirst());
    }

    @Test
    void voteSurvivesRestartUnlessForgotten() {
        Cluster c = new Cluster(3);
        int leader = c.electLeader();
        assertNotEquals(RaftNode.NONE, c.hardStates.get(leader).votedFor());

        Cluster bug = new Cluster(3, cfg -> cfg.withUnsafe(Set.of(Unsafe.FORGET_VOTE)));
        int l = bug.electLeader();
        assertEquals(RaftNode.NONE, bug.hardStates.get(l).votedFor());
    }
}
