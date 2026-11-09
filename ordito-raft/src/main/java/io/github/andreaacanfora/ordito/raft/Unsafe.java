package io.github.andreaacanfora.ordito.raft;

/**
 * Deliberate bugs, off by default. They exist so the simulator and the checker can show what they catch:
 * turn one on and run {@code ordito-sim --bug <name>} to watch Raft break and get the seed that breaks it.
 */
public enum Unsafe {

    /** The vote is not written to disk, so a node that restarts can vote twice in the same term. */
    FORGET_VOTE,

    /** Votes are granted without checking that the candidate's log is up to date. */
    VOTE_FOR_STALE_LOG,

    /** Entries from earlier terms are committed by counting replicas (the bug in figure 8 of the paper). */
    COMMIT_PREVIOUS_TERM,

    /** Reads are served at the leader's commit index without confirming it is still the leader. */
    STALE_READS
}
