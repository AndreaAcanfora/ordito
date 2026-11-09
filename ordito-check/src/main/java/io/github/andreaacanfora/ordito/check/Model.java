package io.github.andreaacanfora.ordito.check;

import java.util.List;

/**
 * A sequential specification: what the system should do if operations ran one at a time. States must
 * be immutable and implement {@code equals} and {@code hashCode}, so the checker can memoize them.
 */
public interface Model<S, I, O> {

    S initial();

    /**
     * The state after {@code input} runs in {@code state} and replies {@code output}, or null if that
     * reply is impossible in that state. {@code output} is null when the reply is unknown: then any
     * outcome is possible and the operation's effect must be applied.
     */
    S step(S state, I input, O output);

    /**
     * Splits a history into independent parts checked separately, for example one part per key of a
     * key-value store. Checking parts separately is exponentially cheaper.
     */
    default List<List<Operation<I, O>>> partition(List<Operation<I, O>> history) {
        return List.of(history);
    }

    default String describe(I input, O output) {
        return input + " -> " + (output == null ? "?" : output);
    }
}
