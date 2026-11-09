package io.github.andreaacanfora.ordito.raft;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 * Static settings for one node. Times are in ticks: the host decides how long a tick is.
 *
 * @param id this node, a positive number
 * @param members every node of the cluster, including this one
 * @param electionTicks a follower that hears nothing from a leader for between this and twice this
 *     many ticks starts an election
 * @param heartbeatTicks how often a leader sends heartbeats
 * @param maxBatch the most entries sent in one message
 * @param preVote ask "would you vote for me?" before starting a real election
 * @param checkQuorum a leader that has not heard from a majority for an election timeout steps down,
 *     and followers ignore vote requests while they hear from a leader
 * @param unsafe deliberate bugs, for the simulator; empty in real use
 */
public record Config(int id, List<Integer> members, int electionTicks, int heartbeatTicks, int maxBatch,
        boolean preVote, boolean checkQuorum, Set<Unsafe> unsafe) {

    public Config {
        members = List.copyOf(members);
        unsafe = unsafe.isEmpty() ? Set.of() : Collections.unmodifiableSet(EnumSet.copyOf(unsafe));
        if (id < 1 || !members.contains(id)) {
            throw new IllegalArgumentException("id must be a positive member of the cluster");
        }
        if (members.stream().distinct().count() != members.size() || members.stream().anyMatch(m -> m < 1)) {
            throw new IllegalArgumentException("members must be distinct positive ids");
        }
        if (heartbeatTicks < 1 || electionTicks <= heartbeatTicks || maxBatch < 1) {
            throw new IllegalArgumentException("need 1 <= heartbeatTicks < electionTicks and maxBatch >= 1");
        }
    }

    public static Config of(int id, List<Integer> members) {
        return new Config(id, members, 10, 2, 64, true, true, Set.of());
    }

    public Config withTicks(int election, int heartbeat) {
        return new Config(id, members, election, heartbeat, maxBatch, preVote, checkQuorum, unsafe);
    }

    public Config withPreVote(boolean on) {
        return new Config(id, members, electionTicks, heartbeatTicks, maxBatch, on, checkQuorum, unsafe);
    }

    public Config withCheckQuorum(boolean on) {
        return new Config(id, members, electionTicks, heartbeatTicks, maxBatch, preVote, on, unsafe);
    }

    public Config withUnsafe(Set<Unsafe> bugs) {
        return new Config(id, members, electionTicks, heartbeatTicks, maxBatch, preVote, checkQuorum, bugs);
    }

    public boolean has(Unsafe bug) {
        return unsafe.contains(bug);
    }
}
