package io.github.andreaacanfora.ordito.raft;

/**
 * What a node must have on disk before it sends any message that depends on it: the current term, who
 * it voted for in that term ({@link RaftNode#NONE} if nobody), and how far the log is known committed.
 */
public record HardState(long term, int votedFor, long commit) {

    public static final HardState INITIAL = new HardState(0, RaftNode.NONE, 0);
}
