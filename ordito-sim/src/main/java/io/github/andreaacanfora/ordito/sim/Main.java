package io.github.andreaacanfora.ordito.sim;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/** Command-line entry point: run one seed in detail, or many seeds in parallel looking for a failure. */
public final class Main {

    private static final String USAGE = """
            ordito-sim: runs a whole Raft cluster in one thread, with faults, from a seed.

            Usage: java -jar ordito-sim.jar [options]
              --seed <n>        run this seed (default: random); a failure prints the seed to replay it
              --runs <n>        run n seeds starting at --seed, in parallel, and stop at the first failure
              --nodes <n>       cluster size (default 5)
              --clients <n>     concurrent clients (default 5)
              --time <ms>       simulated time with faults, before the cluster must recover (default 20000)
              --no-faults       no crashes, pauses or partitions
              --bug <name>      inject a bug (repeatable): %s
              --log             print every event of the run (elections, faults, client operations)
              --help            show this help
            """.formatted(String.join(", ", bugNames()));

    private Main() {}

    public static void main(String[] args) throws Exception {
        long seed = System.nanoTime() & 0xffff_ffffL;
        int runs = 1;
        boolean showLog = false;
        SimConfig base = SimConfig.defaults(0);
        Set<Bug> bugs = EnumSet.noneOf(Bug.class);
        try {
            for (int i = 0; i < args.length; i++) {
                String a = args[i];
                switch (a) {
                    case "--help", "-h" -> {
                        System.out.print(USAGE);
                        return;
                    }
                    case "--no-faults" -> base = base.withFaults(false);
                    case "--log" -> showLog = true;
                    case "--seed", "--runs", "--nodes", "--clients", "--time", "--bug" -> {
                        if (i + 1 >= args.length) {
                            throw new IllegalArgumentException(a + " needs a value");
                        }
                        String v = args[++i];
                        switch (a) {
                            case "--seed" -> seed = Long.parseLong(v);
                            case "--runs" -> runs = Integer.parseInt(v);
                            case "--nodes" -> base = base.withNodes(Integer.parseInt(v));
                            case "--clients" -> base = base.withClients(Integer.parseInt(v));
                            case "--time" -> base = base.withDuration(Long.parseLong(v));
                            default -> bugs.add(Bug.fromCli(v));
                        }
                    }
                    default -> throw new IllegalArgumentException("unknown option " + a);
                }
            }
        } catch (IllegalArgumentException e) {
            System.err.println("error: " + e.getMessage());
            System.err.print(USAGE);
            System.exit(2);
            return;
        }
        base = base.withBugs(bugs);
        for (Bug bug : bugs) {
            System.out.println("injected bug: " + bug.cliName() + " (" + bug.description() + ")");
        }
        int code = runs == 1 ? single(base.withSeed(seed), showLog) : many(base, seed, runs);
        System.exit(code);
    }

    private static int single(SimConfig config, boolean showLog) {
        long start = System.nanoTime();
        Simulation sim = new Simulation(config);
        if (showLog) {
            sim.onLog(System.out::println);
        }
        sim.run();
        long ms = (System.nanoTime() - start) / 1_000_000;
        if (!sim.ok()) {
            report(sim);
            return 1;
        }
        var s = sim.stats();
        System.out.printf(Locale.ROOT, "seed %d: ok. %d nodes, %.1f s simulated in %d ms: %d elections, %d faults, "
                        + "%d messages (%d lost), %d entries committed, %d client operations (%d timed out), "
                        + "history linearizable.%n",
                config.seed(), config.nodes(), sim.now() / 1000.0, ms, s.elections(), s.faults(), s.messages(),
                s.dropped(), s.committedEntries(), s.operations(), s.unknown());
        return 0;
    }

    private static int many(SimConfig base, long firstSeed, int runs) throws Exception {
        int threads = Runtime.getRuntime().availableProcessors();
        System.out.printf("running %d seeds from %d on %d threads...%n", runs, firstSeed, threads);
        long start = System.nanoTime();
        AtomicBoolean stop = new AtomicBoolean();
        AtomicLong done = new AtomicLong();
        AtomicLong simulatedMs = new AtomicLong();
        List<Future<Simulation>> futures = new ArrayList<>();
        try (ExecutorService pool = Executors.newFixedThreadPool(threads)) {
            for (int i = 0; i < runs; i++) {
                long seed = firstSeed + i;
                futures.add(pool.submit(() -> {
                    if (stop.get()) {
                        return null;
                    }
                    Simulation sim = new Simulation(base.withSeed(seed)).run();
                    simulatedMs.addAndGet(sim.now());
                    if (done.incrementAndGet() % 1000 == 0) {
                        System.out.printf("  %d runs...%n", done.get());
                    }
                    if (!sim.ok()) {
                        stop.set(true);
                    }
                    return sim;
                }));
            }
            Simulation firstFailure = null;
            for (Future<Simulation> f : futures) {
                Simulation sim = f.get();
                if (sim != null && !sim.ok() && (firstFailure == null || sim.config().seed() < firstFailure.config().seed())) {
                    firstFailure = sim;
                }
            }
            double seconds = (System.nanoTime() - start) / 1e9;
            if (firstFailure != null) {
                System.out.printf(Locale.ROOT, "%d runs before the failure, %.1f s%n%n", done.get(), seconds);
                report(firstFailure);
                return 1;
            }
            System.out.printf(Locale.ROOT, "%d runs, 0 failures: %.1f hours of cluster time simulated in %.1f s.%n",
                    runs, simulatedMs.get() / 3_600_000.0, seconds);
            return 0;
        }
    }

    private static void report(Simulation sim) {
        System.out.println("FAILED: " + sim.failure() + " in seed " + sim.config().seed());
        System.out.println("  " + sim.failureMessage());
        System.out.println("  environment: " + sim.environment());
        if (sim.linearizability() != null && sim.linearizability().culprit() != null) {
            var r = sim.linearizability();
            System.out.println();
            System.out.println("  the operation that cannot be explained, and the ones concurrent with it:");
            System.out.println("    " + line(r.culprit()) + "   <- this one");
            r.concurrent().stream().limit(12).forEach(op -> System.out.println("    " + line(op)));
        }
        System.out.println();
        System.out.println("  last events:");
        sim.recentLog().stream().skip(Math.max(0, sim.recentLog().size() - 25))
                .forEach(l -> System.out.println("  " + l));
        System.out.println();
        StringBuilder replay = new StringBuilder("java -jar ordito-sim.jar --seed " + sim.config().seed());
        for (Bug bug : sim.bugs()) {
            replay.append(" --bug ").append(bug.cliName());
        }
        if (sim.config().nodes() != 5) {
            replay.append(" --nodes ").append(sim.config().nodes());
        }
        System.out.println("  replay it, event by event: " + replay + " --log");
    }

    @SuppressWarnings("unchecked")
    private static String line(io.github.andreaacanfora.ordito.check.Operation<?, ?> op) {
        var o = (io.github.andreaacanfora.ordito.check.Operation<io.github.andreaacanfora.ordito.check.KvModel.Op,
                io.github.andreaacanfora.ordito.check.KvModel.Reply>) op;
        String when = o.unknown() ? "[%d ms, ?]".formatted(o.called()) : "[%d ms, %d ms]".formatted(o.called(), o.returned());
        return "%-22s client %d  %s".formatted(when, o.client(),
                io.github.andreaacanfora.ordito.check.KvModel.INSTANCE.describe(o.input(), o.output()));
    }

    private static List<String> bugNames() {
        List<String> names = new ArrayList<>();
        for (Bug bug : Bug.values()) {
            names.add(bug.cliName());
        }
        return names;
    }
}
