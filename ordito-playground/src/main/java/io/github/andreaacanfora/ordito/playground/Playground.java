package io.github.andreaacanfora.ordito.playground;

import io.github.andreaacanfora.ordito.check.KvModel;
import io.github.andreaacanfora.ordito.check.Result;
import io.github.andreaacanfora.ordito.raft.Entry;
import io.github.andreaacanfora.ordito.raft.Message;
import io.github.andreaacanfora.ordito.raft.RaftNode;
import io.github.andreaacanfora.ordito.sim.Bug;
import io.github.andreaacanfora.ordito.sim.SimConfig;
import io.github.andreaacanfora.ordito.sim.Simulation;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.teavm.jso.JSExport;

/**
 * The simulator, exported to JavaScript. The page calls these functions; state comes back as JSON.
 * Everything runs in the browser, from the same code as the command-line simulator.
 */
public final class Playground {

    private static Simulation sim;

    private Playground() {}

    public static void main(String[] args) {
        // nothing to do at load: the page calls the exported functions
    }

    /**
     * Starts a simulation. In replay mode it is configured exactly like {@code ordito-sim --seed}, so a
     * failing seed found on the command line fails the same way here. Otherwise it is a sandbox: a
     * steady network, no automatic faults, and no end.
     */
    @JSExport
    public static void orditoCreate(int seed, int nodes, String bug, boolean replay) {
        SimConfig config = SimConfig.defaults(seed).withNodes(nodes);
        if (!bug.isEmpty()) {
            config = config.withBugs(Set.of(Bug.fromCli(bug)));
        }
        if (!replay) {
            config = config.withSwarm(false).withFaults(false).withDuration(1L << 50);
        }
        sim = new Simulation(config);
    }

    @JSExport
    public static void orditoRunTo(double time) {
        sim.runUntil((long) time);
    }

    /** Runs to the end exactly like the command line does, final checks included. */
    @JSExport
    public static void orditoRunAll() {
        sim.run();
    }

    /** The same line as {@code ordito-sim --fingerprint}, to check the browser and the JVM agree. */
    @JSExport
    public static String orditoFingerprint(int seed, int nodes) {
        Simulation s = new Simulation(SimConfig.defaults(seed).withNodes(nodes)).run();
        var st = s.stats();
        return "seed=" + seed + " failure=" + s.failure() + " elections=" + st.elections() + " messages=" + st.messages()
                + " committed=" + st.committedEntries() + " operations=" + st.operations() + " history=" + s.historyDigest();
    }

    @JSExport
    public static double orditoNow() {
        return sim.now();
    }

    @JSExport
    public static void orditoCrash(int id) {
        sim.crash(id);
    }

    @JSExport
    public static void orditoRestart(int id) {
        sim.restart(id);
    }

    @JSExport
    public static void orditoPause(int id, int ms) {
        sim.pause(id, ms);
    }

    @JSExport
    public static void orditoResume(int id) {
        sim.resume(id);
    }

    @JSExport
    public static void orditoIsolate(int id) {
        sim.isolate(id);
    }

    @JSExport
    public static void orditoSetLink(int a, int b, boolean cut) {
        sim.setLink(a, b, cut);
    }

    @JSExport
    public static void orditoHeal() {
        sim.heal();
    }

    @JSExport
    public static void orditoSetFaults(boolean on) {
        sim.setFaultsActive(on);
    }

    @JSExport
    public static void orditoSetClients(boolean on) {
        sim.setClientsActive(on);
    }

    @JSExport
    public static void orditoWrite(String key, String value) {
        sim.clientWrite(key, value);
    }

    /** Checks the clients' history so far. */
    @JSExport
    public static String orditoCheck() {
        Result r = sim.checkHistory();
        StringBuilder j = new StringBuilder("{");
        field(j, "verdict", r.verdict().name()).append(',');
        field(j, "operations", r.operations()).append(',');
        field(j, "states", r.statesExplored()).append(',');
        field(j, "explanation", r.explanation()).append(',');
        j.append("\"culprit\":");
        if (r.culprit() == null) {
            j.append("null");
        } else {
            j.append('[').append(op(r.culprit())).append(']');
        }
        j.append(",\"concurrent\":[");
        for (int i = 0; i < r.concurrent().size() && i < 12; i++) {
            j.append(i == 0 ? "" : ",").append(op(r.concurrent().get(i)));
        }
        return j.append("]}").toString();
    }

    @SuppressWarnings("unchecked")
    private static String op(io.github.andreaacanfora.ordito.check.Operation<?, ?> o) {
        var op = (io.github.andreaacanfora.ordito.check.Operation<KvModel.Op, KvModel.Reply>) o;
        StringBuilder j = new StringBuilder("{");
        field(j, "client", op.client()).append(',');
        field(j, "called", op.called()).append(',');
        field(j, "returned", op.unknown() ? -1 : op.returned()).append(',');
        field(j, "text", KvModel.INSTANCE.describe(op.input(), op.output()));
        return j.append('}').toString();
    }

    /** Everything the page draws, as JSON. */
    @JSExport
    public static String orditoState() {
        StringBuilder j = new StringBuilder(4096).append('{');
        field(j, "now", sim.now()).append(',');
        field(j, "failure", sim.failure().name()).append(',');
        field(j, "failureMessage", sim.failureMessage()).append(',');
        field(j, "faults", sim.faultsActive()).append(',');
        field(j, "clients", sim.clientsActive()).append(',');
        field(j, "committed", sim.committedIndex()).append(',');
        var stats = sim.stats();
        field(j, "operations", stats.operations()).append(',');
        field(j, "elections", stats.elections()).append(',');
        field(j, "messageCount", stats.messages()).append(',');
        field(j, "environment", sim.environment().toString()).append(',');
        List<Integer> ids = sim.nodeIds();
        j.append("\"nodes\":[");
        for (int i = 0; i < ids.size(); i++) {
            Simulation.Node n = sim.node(ids.get(i));
            RaftNode r = n.raft();
            j.append(i == 0 ? "{" : ",{");
            field(j, "id", n.id()).append(',');
            field(j, "up", n.up()).append(',');
            field(j, "paused", n.paused()).append(',');
            field(j, "tickMs", n.tickMs()).append(',');
            if (r != null) {
                field(j, "role", r.role().name()).append(',');
                field(j, "term", r.term()).append(',');
                field(j, "leader", r.leader()).append(',');
                field(j, "votedFor", r.votedFor()).append(',');
                field(j, "commit", r.commitIndex()).append(',');
                field(j, "lastIndex", r.lastIndex()).append(',');
                long from = Math.max(1, r.lastIndex() - 39);
                j.append("\"log\":[");
                List<Entry> entries = r.entries(from, r.lastIndex());
                for (int k = 0; k < entries.size(); k++) {
                    Entry e = entries.get(k);
                    j.append(k == 0 ? "[" : ",[").append(e.index()).append(',').append(e.term()).append(',');
                    string(j, Simulation.label(e));
                    j.append(']');
                }
                j.append("],");
            }
            field(j, "applied", n.applied()).append(',');
            j.append("\"kv\":{");
            int k = 0;
            for (Map.Entry<String, String> e : n.kv().entrySet()) {
                j.append(k++ == 0 ? "" : ",");
                string(j, e.getKey());
                j.append(':');
                string(j, e.getValue());
            }
            j.append("}}");
        }
        j.append("],\"cut\":[");
        boolean first = true;
        for (int a : ids) {
            for (int b : ids) {
                if (a < b && (sim.linkBlocked(a, b) || sim.linkBlocked(b, a))) {
                    j.append(first ? "" : ",").append('[').append(a).append(',').append(b).append(']');
                    first = false;
                }
            }
        }
        j.append("],\"messages\":[");
        List<Simulation.InFlight> flying = sim.inFlight();
        for (int i = 0; i < flying.size() && i < 400; i++) {
            Simulation.InFlight f = flying.get(i);
            Message m = f.message();
            j.append(i == 0 ? "[" : ",[").append(m.from()).append(',').append(m.to()).append(',').append(f.sent())
                    .append(',').append(f.arrives()).append(',');
            string(j, kind(m));
            j.append(',').append(m.term()).append(']');
        }
        j.append("],\"log\":[");
        List<String> recent = sim.recentLog();
        int start = Math.max(0, recent.size() - 14);
        for (int i = start; i < recent.size(); i++) {
            j.append(i == start ? "" : ",");
            string(j, recent.get(i).trim());
        }
        return j.append("]}").toString();
    }

    private static String kind(Message m) {
        return switch (m) {
            case Message.RequestVote v -> v.preVote() ? "prevote" : "vote";
            case Message.VoteResponse v -> v.granted() ? "granted" : "denied";
            case Message.AppendEntries a -> a.entries().isEmpty() ? "heartbeat" : "append:" + a.entries().size();
            case Message.AppendResponse r -> r.success() ? "ack" : "nack";
        };
    }

    private static StringBuilder field(StringBuilder j, String name, String value) {
        string(j, name);
        j.append(':');
        string(j, value);
        return j;
    }

    private static StringBuilder field(StringBuilder j, String name, long value) {
        string(j, name);
        return j.append(':').append(value);
    }

    private static StringBuilder field(StringBuilder j, String name, boolean value) {
        string(j, name);
        return j.append(':').append(value);
    }

    private static void string(StringBuilder j, String s) {
        j.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> j.append("\\\"");
                case '\\' -> j.append("\\\\");
                case '\n' -> j.append("\\n");
                default -> {
                    if (c < 0x20) {
                        j.append(' ');
                    } else {
                        j.append(c);
                    }
                }
            }
        }
        j.append('"');
    }
}
