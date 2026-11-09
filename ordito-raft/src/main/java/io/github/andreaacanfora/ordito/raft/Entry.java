package io.github.andreaacanfora.ordito.raft;

import java.util.Arrays;

/** One log entry. An empty {@code data} is a no-op, which a new leader appends to commit its term. */
public record Entry(long index, long term, byte[] data) {

    public Entry {
        if (index < 1 || term < 1 || data == null) {
            throw new IllegalArgumentException("invalid entry " + index + "@" + term);
        }
    }

    public boolean isNoop() {
        return data.length == 0;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Entry e && e.index == index && e.term == term && Arrays.equals(e.data, data);
    }

    @Override
    public int hashCode() {
        return Long.hashCode(index) * 31 + Long.hashCode(term) * 17 + Arrays.hashCode(data);
    }

    @Override
    public String toString() {
        return index + "@" + term + (isNoop() ? "(noop)" : "(" + data.length + "B)");
    }
}
