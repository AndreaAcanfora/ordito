package io.github.andreaacanfora.ordito.raft;

/**
 * A confirmed linearizable read: once the state machine has applied {@code index}, the read with this id
 * can be served from local state and will see every write that completed before it started.
 */
public record ReadState(long requestId, long index) {}
