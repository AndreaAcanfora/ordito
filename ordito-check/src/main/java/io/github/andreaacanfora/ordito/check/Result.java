package io.github.andreaacanfora.ordito.check;

import java.util.List;

/**
 * The verdict. When a history is not linearizable, {@code culprit} is an operation that could not be
 * placed anywhere consistent with the others, {@code concurrent} the operations that overlapped it, and
 * {@code explanation} says so in words.
 */
public record Result(Verdict verdict, int operations, long statesExplored, Operation<?, ?> culprit,
        List<Operation<?, ?>> concurrent, String explanation) {

    public enum Verdict { LINEARIZABLE, NOT_LINEARIZABLE, GAVE_UP }

    public boolean ok() {
        return verdict == Verdict.LINEARIZABLE;
    }
}
