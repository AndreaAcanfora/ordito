package io.github.andreaacanfora.ordito.raft;

import java.util.List;

/**
 * Everything a node wants done, collected since the last call to {@link RaftNode#ready()}. The host must
 * handle it in this order:
 *
 * <ol>
 *   <li>write {@code hardState} (if not null) and the log changes to disk, and sync;
 *   <li>send {@code messages};
 *   <li>apply {@code committed} to the state machine, in order;
 *   <li>answer {@code reads}: each one is safe to serve now that {@code committed} is applied.
 * </ol>
 *
 * Log changes: if {@code persistFrom} is positive, drop every stored entry with index &gt;= persistFrom,
 * then append {@code entries} (which start at persistFrom).
 */
public record Ready(HardState hardState, long persistFrom, List<Entry> entries, List<Message> messages,
        List<Entry> committed, List<ReadState> reads) {

    public boolean isEmpty() {
        return hardState == null && persistFrom == 0 && messages.isEmpty() && committed.isEmpty() && reads.isEmpty();
    }
}
