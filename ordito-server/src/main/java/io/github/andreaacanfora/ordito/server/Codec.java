package io.github.andreaacanfora.ordito.server;

import io.github.andreaacanfora.ordito.raft.Entry;
import io.github.andreaacanfora.ordito.raft.Message;
import io.github.andreaacanfora.ordito.raft.Message.AppendEntries;
import io.github.andreaacanfora.ordito.raft.Message.AppendResponse;
import io.github.andreaacanfora.ordito.raft.Message.RequestVote;
import io.github.andreaacanfora.ordito.raft.Message.VoteResponse;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

/** The wire format between nodes: one byte of type, then the fields in order. */
final class Codec {

    private static final int MAX_ENTRY = 64 * 1024 * 1024;

    private Codec() {}

    static byte[] encode(Message m) {
        var bytes = new ByteArrayOutputStream(64);
        try (var out = new DataOutputStream(bytes)) {
            switch (m) {
                case RequestVote v -> {
                    out.writeByte(1);
                    header(out, m);
                    out.writeLong(v.lastLogIndex());
                    out.writeLong(v.lastLogTerm());
                    out.writeBoolean(v.preVote());
                }
                case VoteResponse v -> {
                    out.writeByte(2);
                    header(out, m);
                    out.writeBoolean(v.granted());
                    out.writeBoolean(v.preVote());
                }
                case AppendEntries a -> {
                    out.writeByte(3);
                    header(out, m);
                    out.writeLong(a.prevLogIndex());
                    out.writeLong(a.prevLogTerm());
                    out.writeLong(a.leaderCommit());
                    out.writeLong(a.readSeq());
                    out.writeInt(a.entries().size());
                    for (Entry e : a.entries()) {
                        out.writeLong(e.index());
                        out.writeLong(e.term());
                        out.writeInt(e.data().length);
                        out.write(e.data());
                    }
                }
                case AppendResponse r -> {
                    out.writeByte(4);
                    header(out, m);
                    out.writeBoolean(r.success());
                    out.writeLong(r.index());
                    out.writeLong(r.readSeq());
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return bytes.toByteArray();
    }

    private static void header(DataOutputStream out, Message m) throws IOException {
        out.writeInt(m.from());
        out.writeInt(m.to());
        out.writeLong(m.term());
    }

    static Message decode(byte[] frame) throws IOException {
        var in = new DataInputStream(new ByteArrayInputStream(frame));
        int type = in.readUnsignedByte();
        int from = in.readInt();
        int to = in.readInt();
        long term = in.readLong();
        return switch (type) {
            case 1 -> new RequestVote(from, to, term, in.readLong(), in.readLong(), in.readBoolean());
            case 2 -> new VoteResponse(from, to, term, in.readBoolean(), in.readBoolean());
            case 3 -> {
                long prev = in.readLong();
                long prevTerm = in.readLong();
                long commit = in.readLong();
                long readSeq = in.readLong();
                int n = in.readInt();
                List<Entry> entries = new ArrayList<>(Math.min(n, 1024));
                for (int i = 0; i < n; i++) {
                    long index = in.readLong();
                    long t = in.readLong();
                    int len = in.readInt();
                    if (len < 0 || len > MAX_ENTRY) {
                        throw new IOException("bad entry length " + len);
                    }
                    entries.add(new Entry(index, t, in.readNBytes(len)));
                }
                yield new AppendEntries(from, to, term, prev, prevTerm, entries, commit, readSeq);
            }
            case 4 -> new AppendResponse(from, to, term, in.readBoolean(), in.readLong(), in.readLong());
            default -> throw new IOException("unknown message type " + type);
        };
    }
}
