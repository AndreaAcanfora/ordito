package io.github.andreaacanfora.ordito.check;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A key-value store of string registers: get, put, compare-and-set and swap (Redis' {@code SET k v GET}).
 * Keys are independent, so histories are checked one key at a time.
 */
public final class KvModel implements Model<KvModel.Register, KvModel.Op, KvModel.Reply> {

    public static final KvModel INSTANCE = new KvModel();

    public enum Kind { GET, PUT, CAS, SWAP }

    /** An operation on one key. {@code value} is what PUT, CAS and SWAP write; {@code expected} is CAS's. */
    public record Op(Kind kind, String key, String value, String expected) {

        public static Op get(String key) {
            return new Op(Kind.GET, key, null, null);
        }

        public static Op put(String key, String value) {
            return new Op(Kind.PUT, key, value, null);
        }

        public static Op cas(String key, String expected, String value) {
            return new Op(Kind.CAS, key, value, expected);
        }

        public static Op swap(String key, String value) {
            return new Op(Kind.SWAP, key, value, null);
        }
    }

    /** GET and SWAP reply with a value (null: the key was absent); PUT and CAS with whether they took effect. */
    public record Reply(String value, boolean ok) {

        public static Reply value(String value) {
            return new Reply(value, true);
        }

        public static final Reply OK = new Reply(null, true);
        public static final Reply FAILED = new Reply(null, false);
    }

    /** The value of one key; {@code value} null means absent. */
    public record Register(String value) {}

    private static final Register ABSENT = new Register(null);

    private KvModel() {}

    @Override
    public Register initial() {
        return ABSENT;
    }

    @Override
    public Register step(Register state, Op op, Reply reply) {
        return switch (op.kind()) {
            case GET -> reply == null || Objects.equals(reply.value(), state.value()) ? state : null;
            case PUT -> new Register(op.value());
            case CAS -> {
                boolean matches = Objects.equals(op.expected(), state.value());
                if (reply == null) {
                    yield matches ? new Register(op.value()) : state;
                }
                yield reply.ok() == matches ? (matches ? new Register(op.value()) : state) : null;
            }
            case SWAP -> reply == null || Objects.equals(reply.value(), state.value()) ? new Register(op.value()) : null;
        };
    }

    @Override
    public List<List<Operation<Op, Reply>>> partition(List<Operation<Op, Reply>> history) {
        Map<String, List<Operation<Op, Reply>>> byKey = new LinkedHashMap<>();
        for (Operation<Op, Reply> op : history) {
            byKey.computeIfAbsent(op.input().key(), k -> new ArrayList<>()).add(op);
        }
        return List.copyOf(byKey.values());
    }

    @Override
    public String describe(Op op, Reply reply) {
        String call = switch (op.kind()) {
            case GET -> "get(" + op.key() + ")";
            case PUT -> "put(" + op.key() + ", " + op.value() + ")";
            case CAS -> "cas(" + op.key() + ", " + (op.expected() == null ? "nil" : op.expected()) + " -> " + op.value() + ")";
            case SWAP -> "swap(" + op.key() + ", " + op.value() + ")";
        };
        if (reply == null) {
            return call + " -> ?";
        }
        return call + " -> " + switch (op.kind()) {
            case GET, SWAP -> reply.value() == null ? "nil" : reply.value();
            case PUT -> "ok";
            case CAS -> reply.ok() ? "ok" : "failed";
        };
    }
}
