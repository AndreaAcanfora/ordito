package io.github.andreaacanfora.ordito.sim;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

/**
 * One simulated run. Times are simulated milliseconds.
 *
 * @param seed decides everything: elections, latencies, faults and client operations
 * @param nodes cluster size
 * @param clients concurrent clients, each with one operation in flight at a time
 * @param keys how many keys the clients work on (fewer keys, more contention)
 * @param durationMs how long faults are injected; then the network heals and the cluster must recover
 * @param faults inject crashes, pauses, partitions and clock skew
 * @param dropRate probability that a message between nodes is lost
 * @param duplicateRate probability that a message between nodes is delivered twice
 * @param maxLatencyMs messages take 1 to this many ms, so they also arrive out of order
 * @param bugs deliberate bugs to inject
 * @param swarm let the seed also pick the environment (message loss, latency, batch size, how often
 *     faults strike), overriding the values above: "swarm testing", which varies the conditions so that
 *     rare interleavings come up in some runs instead of never
 */
public record SimConfig(long seed, int nodes, int clients, int keys, long durationMs, boolean faults,
        double dropRate, double duplicateRate, int maxLatencyMs, Set<Bug> bugs, boolean swarm) {

    public SimConfig {
        bugs = bugs.isEmpty() ? Set.of() : Collections.unmodifiableSet(EnumSet.copyOf(bugs));
        if (nodes < 1 || clients < 0 || keys < 1 || durationMs < 0 || maxLatencyMs < 1) {
            throw new IllegalArgumentException("invalid simulation settings");
        }
    }

    public static SimConfig defaults(long seed) {
        return new SimConfig(seed, 5, 5, 3, 20_000, true, 0.02, 0.01, 15, Set.of(), true);
    }

    public SimConfig withSeed(long s) {
        return new SimConfig(s, nodes, clients, keys, durationMs, faults, dropRate, duplicateRate, maxLatencyMs, bugs, swarm);
    }

    public SimConfig withNodes(int n) {
        return new SimConfig(seed, n, clients, keys, durationMs, faults, dropRate, duplicateRate, maxLatencyMs, bugs, swarm);
    }

    public SimConfig withClients(int c) {
        return new SimConfig(seed, nodes, c, keys, durationMs, faults, dropRate, duplicateRate, maxLatencyMs, bugs, swarm);
    }

    public SimConfig withDuration(long ms) {
        return new SimConfig(seed, nodes, clients, keys, ms, faults, dropRate, duplicateRate, maxLatencyMs, bugs, swarm);
    }

    public SimConfig withFaults(boolean on) {
        return new SimConfig(seed, nodes, clients, keys, durationMs, on, dropRate, duplicateRate, maxLatencyMs, bugs, swarm);
    }

    public SimConfig withBugs(Set<Bug> b) {
        return new SimConfig(seed, nodes, clients, keys, durationMs, faults, dropRate, duplicateRate, maxLatencyMs, b, swarm);
    }

    public SimConfig withSwarm(boolean on) {
        return new SimConfig(seed, nodes, clients, keys, durationMs, faults, dropRate, duplicateRate, maxLatencyMs, bugs, on);
    }

    public boolean has(Bug bug) {
        return bugs.contains(bug);
    }
}
