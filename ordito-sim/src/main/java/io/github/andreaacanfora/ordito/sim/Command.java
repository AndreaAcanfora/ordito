package io.github.andreaacanfora.ordito.sim;

import io.github.andreaacanfora.ordito.check.KvModel.Kind;
import io.github.andreaacanfora.ordito.check.KvModel.Op;
import java.nio.charset.StandardCharsets;

/** A client operation as stored in a log entry: who sent it, and what to do. */
record Command(int client, long opId, Op op) {

    private static final char SEP = '\u001f';

    byte[] encode() {
        String text = client + "" + SEP + opId + SEP + op.kind() + SEP + op.key() + SEP + nz(op.value()) + SEP
                + nz(op.expected());
        return text.getBytes(StandardCharsets.UTF_8);
    }

    static Command decode(byte[] data) {
        String[] f = new String(data, StandardCharsets.UTF_8).split(String.valueOf(SEP), -1);
        Op op = new Op(Kind.valueOf(f[2]), f[3], zn(f[4]), zn(f[5]));
        return new Command(Integer.parseInt(f[0]), Long.parseLong(f[1]), op);
    }

    private static String nz(String s) {
        return s == null ? "\u0000" : s;
    }

    private static String zn(String s) {
        return s.equals("\u0000") ? null : s;
    }
}
