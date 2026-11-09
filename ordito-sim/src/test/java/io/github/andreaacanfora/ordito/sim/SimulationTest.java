package io.github.andreaacanfora.ordito.sim;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class SimulationTest {

    @Test
    void aSeedReplaysExactly() {
        List<String> first = new ArrayList<>();
        List<String> second = new ArrayList<>();
        Simulation a = new Simulation(SimConfig.defaults(42)).onLog(first::add).run();
        Simulation b = new Simulation(SimConfig.defaults(42)).onLog(second::add).run();
        assertEquals(first, second);
        assertEquals(a.stats(), b.stats());
        assertEquals(a.history(), b.history());
    }

    @Test
    void differentSeedsDiffer() {
        assertTrue(!new Simulation(SimConfig.defaults(1)).run().stats()
                .equals(new Simulation(SimConfig.defaults(2)).run().stats()));
    }

    @ParameterizedTest
    @ValueSource(ints = {3, 5})
    void manySeedsWithFaultsStaySafeAndLinearizable(int nodes) {
        for (long seed = 1; seed <= 150; seed++) {
            Simulation sim = new Simulation(SimConfig.defaults(seed).withNodes(nodes)).run();
            assertTrue(sim.ok(), () -> "seed " + sim.config().seed() + ": " + sim.failure() + " " + sim.failureMessage()
                    + "\n" + String.join("\n", sim.recentLog()));
            assertTrue(sim.stats().operations() > 0);
        }
    }

    @Test
    void withoutFaultsNothingTimesOut() {
        Simulation sim = new Simulation(SimConfig.defaults(7).withFaults(false).withSwarm(false)).run();
        assertTrue(sim.ok());
        assertEquals(1, sim.stats().elections());
    }

    @Test
    void singleNodeClusterWorks() {
        Simulation sim = new Simulation(SimConfig.defaults(3).withNodes(1)).run();
        assertTrue(sim.ok(), sim.failureMessage());
    }

    /** Each injected bug must be caught. These are the first seeds that catch it (bug:nodes:seed:failure). */
    @ParameterizedTest
    @ValueSource(strings = {
            "vote-for-stale-log:5:1:LEADER_COMPLETENESS",
            "stale-reads:5:8:LINEARIZABILITY",
            "lying-fsync:3:1:LEADER_COMPLETENESS",
            "forget-vote:3:146:ELECTION_SAFETY",
            "commit-previous-term:3:1071:LEADER_COMPLETENESS"})
    void injectedBugsAreCaught(String spec) {
        String[] f = spec.split(":");
        SimConfig config = SimConfig.defaults(Long.parseLong(f[2])).withNodes(Integer.parseInt(f[1]))
                .withBugs(Set.of(Bug.fromCli(f[0])));
        Simulation sim = new Simulation(config).run();
        assertEquals(Simulation.Failure.valueOf(f[3]), sim.failure(), sim.failureMessage());

        Simulation clean = new Simulation(config.withBugs(Set.of())).run();
        assertTrue(clean.ok(), "the same seed passes without the bug");
    }
}
