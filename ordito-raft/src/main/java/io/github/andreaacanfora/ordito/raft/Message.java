package io.github.andreaacanfora.ordito.raft;

import java.util.List;

/** The four Raft messages. Heartbeats are {@link AppendEntries} with no entries. */
public sealed interface Message {

    int from();

    int to();

    long term();

    /**
     * Asks for a vote. A pre-vote ({@code preVote} true) asks "would you vote for me?" for {@code term}
     * without anybody changing their term, so a node cut off from the cluster cannot disrupt it when it
     * comes back.
     */
    record RequestVote(int from, int to, long term, long lastLogIndex, long lastLogTerm, boolean preVote)
            implements Message {}

    record VoteResponse(int from, int to, long term, boolean granted, boolean preVote) implements Message {}

    /**
     * Replicates {@code entries} after the entry at {@code prevLogIndex}. {@code readSeq} numbers the
     * leader's heartbeat rounds; echoing it back proves the leader was still leader at that point, which
     * is how linearizable reads are confirmed (ReadIndex).
     */
    record AppendEntries(int from, int to, long term, long prevLogIndex, long prevLogTerm, List<Entry> entries,
            long leaderCommit, long readSeq) implements Message {

        public AppendEntries {
            entries = List.copyOf(entries);
        }
    }

    /**
     * On success {@code index} is the last index known to match the leader. On failure it is a hint: the
     * first index the leader should try next (it skips a whole conflicting term at a time).
     */
    record AppendResponse(int from, int to, long term, boolean success, long index, long readSeq)
            implements Message {}
}
