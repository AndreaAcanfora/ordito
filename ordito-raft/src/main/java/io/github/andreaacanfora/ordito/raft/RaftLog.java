package io.github.andreaacanfora.ordito.raft;

import java.util.ArrayList;
import java.util.List;

/** The in-memory log. Index 1 is the first entry; index 0 is the empty prefix, with term 0. */
final class RaftLog {

    private final List<Entry> entries = new ArrayList<>();

    RaftLog(List<Entry> restored) {
        long expected = 1;
        for (Entry e : restored) {
            if (e.index() != expected++) {
                throw new IllegalArgumentException("log has a gap before " + e);
            }
            entries.add(e);
        }
    }

    long lastIndex() {
        return entries.size();
    }

    long lastTerm() {
        return term(lastIndex());
    }

    /** The term at an index: 0 for index 0, -1 if there is no such entry. */
    long term(long index) {
        if (index == 0) {
            return 0;
        }
        if (index < 0 || index > entries.size()) {
            return -1;
        }
        return entries.get((int) index - 1).term();
    }

    boolean matches(long index, long term) {
        return term(index) == term;
    }

    Entry get(long index) {
        return entries.get((int) index - 1);
    }

    /** Entries in [from, to], inclusive; empty if from &gt; to. */
    List<Entry> slice(long from, long to) {
        if (from > to) {
            return List.of();
        }
        return List.copyOf(entries.subList((int) from - 1, (int) to));
    }

    void append(Entry entry) {
        if (entry.index() != lastIndex() + 1) {
            throw new IllegalStateException("append " + entry + " after " + lastIndex());
        }
        entries.add(entry);
    }

    /** Removes the entry at {@code index} and everything after it. */
    void truncateFrom(long index) {
        entries.subList((int) index - 1, entries.size()).clear();
    }

    /** The first index of the term that {@code index} belongs to, not going below {@code floor}. */
    long firstIndexOfTerm(long index, long floor) {
        long term = term(index);
        long i = index;
        while (i - 1 > floor && term(i - 1) == term) {
            i--;
        }
        return i;
    }
}
