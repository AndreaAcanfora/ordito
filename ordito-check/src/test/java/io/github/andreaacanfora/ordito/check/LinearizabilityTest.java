package io.github.andreaacanfora.ordito.check;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.andreaacanfora.ordito.check.KvModel.Op;
import io.github.andreaacanfora.ordito.check.KvModel.Reply;
import io.github.andreaacanfora.ordito.check.Result.Verdict;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;

class LinearizabilityTest {

    private final List<Operation<Op, Reply>> h = new ArrayList<>();

    private void op(int client, Op input, Reply output, long called, long returned) {
        h.add(new Operation<>(h.size(), client, input, output, called, returned));
    }

    private Result check() {
        return Linearizability.check(KvModel.INSTANCE, h);
    }

    @Test
    void sequentialHistoryIsLinearizable() {
        op(1, Op.put("x", "1"), Reply.OK, 0, 1);
        op(1, Op.get("x"), Reply.value("1"), 2, 3);
        op(2, Op.cas("x", "1", "2"), Reply.OK, 4, 5);
        op(2, Op.swap("x", "3"), Reply.value("2"), 6, 7);
        op(1, Op.get("x"), Reply.value("3"), 8, 9);
        assertTrue(check().ok());
    }

    @Test
    void concurrentOperationsMayTakeEffectInEitherOrder() {
        op(1, Op.put("x", "1"), Reply.OK, 0, 10);
        op(2, Op.put("x", "2"), Reply.OK, 1, 9);
        op(3, Op.get("x"), Reply.value("1"), 11, 12); // 2 then 1
        assertTrue(check().ok());
    }

    @Test
    void staleReadAfterACompletedWriteIsCaught() {
        op(1, Op.put("x", "1"), Reply.OK, 0, 1);
        op(1, Op.put("x", "2"), Reply.OK, 2, 3);
        op(2, Op.get("x"), Reply.value("1"), 4, 5); // the write of 2 had already completed
        Result r = check();
        assertEquals(Verdict.NOT_LINEARIZABLE, r.verdict());
        assertEquals(2, r.culprit().id());
        assertTrue(r.explanation().contains("get(x) -> 1"), r.explanation());
    }

    @Test
    void readsCannotGoBackInTime() {
        op(1, Op.put("x", "1"), Reply.OK, 0, 100); // slow write
        op(2, Op.get("x"), Reply.value("1"), 10, 20); // sees it
        op(3, Op.get("x"), Reply.value(null), 30, 40); // then a later read does not
        assertFalse(check().ok());
    }

    @Test
    void unknownWriteMayOrMayNotHaveHappened() {
        op(1, Op.put("x", "1"), null, 0, Operation.NEVER);
        op(2, Op.get("x"), Reply.value(null), 5, 6);
        op(2, Op.get("x"), Reply.value("1"), 7, 8);
        assertTrue(check().ok());

        h.clear();
        op(1, Op.put("x", "1"), null, 0, Operation.NEVER);
        op(2, Op.get("x"), Reply.value("1"), 5, 6);
        op(2, Op.get("x"), Reply.value(null), 7, 8); // but once seen it cannot vanish
        assertFalse(check().ok());
    }

    @Test
    void failedCasMustSeeAnotherValue() {
        op(1, Op.put("x", "a"), Reply.OK, 0, 1);
        op(2, Op.cas("x", "a", "b"), Reply.FAILED, 2, 3);
        assertFalse(check().ok());
    }

    @Test
    void keysAreCheckedIndependently() {
        op(1, Op.put("x", "1"), Reply.OK, 0, 1);
        op(2, Op.put("y", "1"), Reply.OK, 0, 1);
        op(1, Op.get("y"), Reply.value("1"), 2, 3);
        op(2, Op.get("x"), Reply.value("1"), 2, 3);
        assertTrue(check().ok());
    }

    @Test
    void largeRandomLinearizableHistoryIsFast() {
        // Simulate an atomic register with real concurrency: each op takes effect at a random instant.
        Random random = new Random(1);
        String[] value = new String[3];
        long[] clientFree = new long[8];
        record Pending(int client, Op op, long called, long point, long returned) {}
        List<Pending> pending = new ArrayList<>();
        for (int i = 0; i < 3000; i++) {
            int c = random.nextInt(clientFree.length);
            long called = clientFree[c] + random.nextInt(5);
            long point = called + random.nextInt(10);
            long returned = point + random.nextInt(10);
            clientFree[c] = returned + 1;
            String key = "k" + random.nextInt(3);
            Op op = switch (random.nextInt(3)) {
                case 0 -> Op.get(key);
                case 1 -> Op.put(key, Integer.toString(i));
                default -> Op.swap(key, Integer.toString(i));
            };
            pending.add(new Pending(c, op, called, point, returned));
        }
        pending.sort((a, b) -> Long.compare(a.point(), b.point()));
        for (Pending p : pending) {
            int k = p.op().key().charAt(1) - '0';
            Reply reply = switch (p.op().kind()) {
                case GET -> Reply.value(value[k]);
                case PUT -> Reply.OK;
                default -> Reply.value(value[k]);
            };
            if (p.op().kind() != KvModel.Kind.GET) {
                value[k] = p.op().value();
            }
            op(p.client(), p.op(), reply, p.called(), p.returned());
        }
        long start = System.nanoTime();
        Result r = check();
        assertTrue(r.ok(), r.explanation());
        assertTrue(System.nanoTime() - start < 5_000_000_000L, "took too long");
    }
}
