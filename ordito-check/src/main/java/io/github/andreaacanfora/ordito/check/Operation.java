package io.github.andreaacanfora.ordito.check;

/**
 * One client operation as observed from outside: what was asked, what came back, and when.
 *
 * <p>An operation whose outcome is unknown (the client timed out, or the connection broke) has
 * {@code output} null and {@code returned} {@link #NEVER}: it may have taken effect at any point after
 * it was called, or not at all, and the checker considers both.
 *
 * @param id unique within a history, for reports
 * @param client which client issued it; one client has at most one operation in flight
 * @param called when the call started
 * @param returned when the reply arrived, or {@link #NEVER}
 */
public record Operation<I, O>(int id, int client, I input, O output, long called, long returned) {

    public static final long NEVER = Long.MAX_VALUE;

    public Operation {
        if (returned < called) {
            throw new IllegalArgumentException("operation " + id + " returned before it was called");
        }
    }

    public boolean unknown() {
        return returned == NEVER;
    }
}
