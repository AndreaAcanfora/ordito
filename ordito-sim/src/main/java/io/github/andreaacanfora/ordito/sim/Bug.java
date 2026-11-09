package io.github.andreaacanfora.ordito.sim;

import io.github.andreaacanfora.ordito.raft.Unsafe;
import java.util.Locale;

/** Bugs the simulator can inject: the ones in {@link Unsafe}, plus a disk that lies about fsync. */
public enum Bug {

    FORGET_VOTE(Unsafe.FORGET_VOTE, "the vote is not saved, so a restarted node can vote twice in one term"),
    VOTE_FOR_STALE_LOG(Unsafe.VOTE_FOR_STALE_LOG, "votes ignore whether the candidate's log is up to date"),
    COMMIT_PREVIOUS_TERM(Unsafe.COMMIT_PREVIOUS_TERM, "old-term entries are committed by counting replicas (figure 8)"),
    STALE_READS(Unsafe.STALE_READS, "the leader serves reads without confirming it is still the leader"),
    LYING_FSYNC(null, "the disk acknowledges writes it has not made durable; a crash loses them");

    private final Unsafe unsafe;
    private final String description;

    Bug(Unsafe unsafe, String description) {
        this.unsafe = unsafe;
        this.description = description;
    }

    /** The Raft-level bug, or null for a bug in the simulated environment. */
    public Unsafe unsafe() {
        return unsafe;
    }

    public String description() {
        return description;
    }

    /** The command-line spelling, like {@code forget-vote}. */
    public String cliName() {
        return name().toLowerCase(Locale.ROOT).replace('_', '-');
    }

    public static Bug fromCli(String name) {
        for (Bug bug : values()) {
            if (bug.cliName().equals(name)) {
                return bug;
            }
        }
        throw new IllegalArgumentException("unknown bug '" + name + "'");
    }
}
