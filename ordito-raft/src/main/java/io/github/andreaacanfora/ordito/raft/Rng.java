package io.github.andreaacanfora.ordito.raft;

/**
 * A small seeded random generator (SplitMix64). Written out by hand instead of using the JDK's, so the
 * same seed gives the same numbers everywhere: on any JVM and in the browser playground.
 */
public final class Rng {

    private long state;

    public Rng(long seed) {
        this.state = seed;
    }

    public long nextLong() {
        long z = (state += 0x9E3779B97F4A7C15L);
        z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
        z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
        return z ^ (z >>> 31);
    }

    /** Uniform in [0, bound), without modulo bias. */
    public int nextInt(int bound) {
        if (bound <= 0) {
            throw new IllegalArgumentException("bound must be positive");
        }
        while (true) {
            int bits = (int) (nextLong() >>> 33); // 31 random bits, non-negative
            int value = bits % bound;
            if (bits - value + (bound - 1) >= 0) { // reject the last, incomplete range
                return value;
            }
        }
    }

    /** Uniform in [from, to]. */
    public int between(int from, int to) {
        return from + nextInt(to - from + 1);
    }

    /** Uniform in [0, 1). */
    public double nextDouble() {
        return (nextLong() >>> 11) * 0x1.0p-53;
    }

    public boolean chance(double probability) {
        return nextDouble() < probability;
    }

    /** An independent generator, so adding draws in one place does not shift the numbers elsewhere. */
    public Rng fork() {
        return new Rng(nextLong());
    }
}
