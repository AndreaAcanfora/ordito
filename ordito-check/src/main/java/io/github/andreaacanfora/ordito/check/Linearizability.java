package io.github.andreaacanfora.ordito.check;

import io.github.andreaacanfora.ordito.check.Result.Verdict;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Decides whether a concurrent history is linearizable: whether every operation can be given one
 * instant between its call and its return such that, in that order, the operations obey the sequential
 * {@link Model}.
 *
 * <p>This is the Wing and Gong search with Lowe's memoization, the algorithm behind Knossos and
 * Porcupine. Calls and returns are put in a linked list by time. The search walks it, tentatively
 * linearizing any operation whose call it reaches and backtracking when it reaches a return whose
 * operation it could not place. Pairs of (operations linearized so far, model state) already seen are
 * skipped, which makes typical histories fast even though the problem is NP-complete.
 */
public final class Linearizability {

    private Linearizability() {}

    public static <S, I, O> Result check(Model<S, I, O> model, List<Operation<I, O>> history) {
        return check(model, history, 10_000_000);
    }

    /** Gives up (with {@link Verdict#GAVE_UP}) after exploring {@code maxStates} states. */
    public static <S, I, O> Result check(Model<S, I, O> model, List<Operation<I, O>> history, long maxStates) {
        long explored = 0;
        for (List<Operation<I, O>> part : model.partition(history)) {
            Result r = checkPart(model, part, maxStates - explored);
            explored += r.statesExplored();
            if (!r.ok()) {
                return new Result(r.verdict(), history.size(), explored, r.culprit(), r.concurrent(), r.explanation());
            }
        }
        return new Result(Verdict.LINEARIZABLE, history.size(), explored, null, List.of(),
                "all " + history.size() + " operations can be ordered consistently");
    }

    private static final class Node {
        final int op; // index into the part
        final boolean call;
        final long time;
        Node match; // a call's return
        Node prev;
        Node next;

        Node(int op, boolean call, long time) {
            this.op = op;
            this.call = call;
            this.time = time;
        }
    }

    private record Frame<S>(Node call, S stateBefore) {}

    private record Seen<S>(BitSet linearized, S state) {}

    private static <S, I, O> Result checkPart(Model<S, I, O> model, List<Operation<I, O>> ops, long budget) {
        int n = ops.size();
        List<Node> events = new ArrayList<>(2 * n);
        for (int i = 0; i < n; i++) {
            Operation<I, O> op = ops.get(i);
            Node call = new Node(i, true, op.called());
            Node ret = new Node(i, false, op.returned());
            call.match = ret;
            events.add(call);
            events.add(ret);
        }
        // By time; at equal times calls first, so operations that touch are treated as concurrent.
        events.sort(Comparator.comparingLong((Node e) -> e.time).thenComparing(e -> !e.call));
        Node head = new Node(-1, false, Long.MIN_VALUE);
        Node tail = head;
        for (Node e : events) {
            tail.next = e;
            e.prev = tail;
            tail = e;
        }

        S state = model.initial();
        BitSet linearized = new BitSet(n);
        Set<Seen<S>> cache = new HashSet<>();
        Deque<Frame<S>> stack = new ArrayDeque<>();
        BitSet best = new BitSet(n);
        long explored = 0;
        Node entry = head.next;
        while (head.next != null) {
            if (explored >= budget) {
                return new Result(Verdict.GAVE_UP, n, explored, null, List.of(),
                        "gave up after exploring " + explored + " states");
            }
            if (entry.call) {
                Operation<I, O> op = ops.get(entry.op);
                S after = model.step(state, op.input(), op.output());
                if (after != null) {
                    BitSet next = (BitSet) linearized.clone();
                    next.set(entry.op);
                    explored++;
                    if (cache.add(new Seen<>(next, after))) {
                        stack.push(new Frame<>(entry, state));
                        state = after;
                        linearized = next;
                        if (linearized.cardinality() > best.cardinality()) {
                            best = (BitSet) linearized.clone();
                        }
                        lift(entry);
                        entry = head.next;
                        continue;
                    }
                }
                entry = entry.next;
            } else {
                // A return whose operation is not linearized yet: everything tried so far is a dead end.
                if (stack.isEmpty()) {
                    return explain(model, ops, best, explored);
                }
                Frame<S> top = stack.pop();
                state = top.stateBefore();
                linearized = (BitSet) linearized.clone();
                linearized.clear(top.call().op);
                unlift(top.call());
                entry = top.call().next;
            }
        }
        return new Result(Verdict.LINEARIZABLE, n, explored, null, List.of(), "linearizable");
    }

    /** Takes an operation's call and return out of the list: it is now placed. */
    private static void lift(Node call) {
        call.prev.next = call.next;
        if (call.next != null) {
            call.next.prev = call.prev;
        }
        Node ret = call.match;
        ret.prev.next = ret.next;
        if (ret.next != null) {
            ret.next.prev = ret.prev;
        }
    }

    /** Puts them back, in reverse order of removal. */
    private static void unlift(Node call) {
        Node ret = call.match;
        ret.prev.next = ret;
        if (ret.next != null) {
            ret.next.prev = ret;
        }
        call.prev.next = call;
        if (call.next != null) {
            call.next.prev = call;
        }
    }

    /**
     * The search failed. The longest consistent prefix it found leaves out some operations; the one of
     * those that returned first had to be placed before its return and could not be.
     */
    private static <S, I, O> Result explain(Model<S, I, O> model, List<Operation<I, O>> ops, BitSet best,
            long explored) {
        Operation<I, O> culprit = null;
        for (int i = 0; i < ops.size(); i++) {
            Operation<I, O> op = ops.get(i);
            if (!best.get(i) && (culprit == null || op.returned() < culprit.returned())) {
                culprit = op;
            }
        }
        List<Operation<?, ?>> concurrent = new ArrayList<>();
        for (Operation<I, O> op : ops) {
            if (op != culprit && op.called() <= culprit.returned() && op.returned() >= culprit.called()) {
                concurrent.add(op);
            }
        }
        StringBuilder why = new StringBuilder();
        why.append("not linearizable: ").append(best.cardinality()).append(" of ").append(ops.size())
                .append(" operations can be ordered, but client ").append(culprit.client()).append("'s ")
                .append(model.describe(culprit.input(), culprit.output()))
                .append(" fits no order of the operations before and around it");
        if (!concurrent.isEmpty()) {
            why.append(" (").append(concurrent.size()).append(" concurrent)");
        }
        return new Result(Verdict.NOT_LINEARIZABLE, ops.size(), explored, culprit, concurrent, why.toString());
    }
}
