package io.github.andreaacanfora.ordito.server;

import io.github.andreaacanfora.ordito.raft.Entry;
import io.github.andreaacanfora.ordito.raft.HardState;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32C;

/**
 * A node's Raft state on disk: {@code raft.log}, one checksummed record per entry, and
 * {@code raft.state}, the hard state, replaced atomically. Every save is fsynced before it returns,
 * because Raft's promises (one vote per term, no lost commits) hold only if the disk keeps what it said
 * it kept.
 *
 * <p>Log record: length (4 bytes, of what follows the checksum), CRC32C (4), index (8), term (8), data.
 * A torn record at the end, from a crash in the middle of a write, is cut off on load.
 */
final class DiskStorage implements Closeable {

    private static final int HEADER = 4 + 4;

    private final Path dir;
    private final FileChannel log;
    private final List<Long> offsets = new ArrayList<>(); // offsets.get(i) = where entry i+1 starts
    private long end;

    /** What was on disk when the node started. */
    record Loaded(HardState hardState, List<Entry> entries, long truncatedBytes) {}

    private final Loaded loaded;
    private HardState written;
    private HardState lazyCommit;

    DiskStorage(Path dir) throws IOException {
        this.dir = dir;
        Files.createDirectories(dir);
        this.log = FileChannel.open(dir.resolve("raft.log"), StandardOpenOption.CREATE, StandardOpenOption.READ,
                StandardOpenOption.WRITE);
        List<Entry> entries = readLog();
        long truncated = log.size() - end;
        if (truncated > 0) {
            log.truncate(end);
            log.force(true);
        }
        loaded = new Loaded(readState(), entries, truncated);
        written = loaded.hardState();
    }

    Loaded loaded() {
        return loaded;
    }

    private List<Entry> readLog() throws IOException {
        List<Entry> entries = new ArrayList<>();
        long size = log.size();
        long pos = 0;
        ByteBuffer header = ByteBuffer.allocate(HEADER);
        while (pos + HEADER <= size) {
            header.clear();
            log.read(header, pos);
            header.flip();
            int length = header.getInt();
            int crc = header.getInt();
            if (length < 16 || pos + HEADER + length > size) {
                break;
            }
            ByteBuffer body = ByteBuffer.allocate(length);
            log.read(body, pos + HEADER);
            CRC32C check = new CRC32C();
            check.update(body.array());
            if ((int) check.getValue() != crc) {
                break;
            }
            body.flip();
            long index = body.getLong();
            long term = body.getLong();
            byte[] data = new byte[length - 16];
            body.get(data);
            if (index != entries.size() + 1) {
                break;
            }
            offsets.add(pos);
            entries.add(new Entry(index, term, data));
            pos += HEADER + length;
        }
        end = pos;
        return entries;
    }

    private HardState readState() throws IOException {
        Path file = dir.resolve("raft.state");
        if (!Files.exists(file)) {
            return HardState.INITIAL;
        }
        ByteBuffer b = ByteBuffer.wrap(Files.readAllBytes(file));
        if (b.remaining() != 8 + 4 + 8 + 4) {
            throw new IOException("corrupt " + file);
        }
        long term = b.getLong();
        int vote = b.getInt();
        long commit = b.getLong();
        int crc = b.getInt();
        CRC32C check = new CRC32C();
        check.update(b.array(), 0, 20);
        if ((int) check.getValue() != crc) {
            throw new IOException("corrupt " + file + " (bad checksum)");
        }
        return new HardState(term, vote, commit);
    }

    /** Drops entries from {@code from} on, appends {@code entries}, writes the hard state; then syncs. */
    void save(HardState state, long from, List<Entry> entries) throws IOException {
        if (from > 0) {
            if (from - 1 < offsets.size()) {
                end = offsets.get((int) from - 1);
                offsets.subList((int) from - 1, offsets.size()).clear();
                log.truncate(end);
            }
            for (Entry e : entries) {
                ByteBuffer body = ByteBuffer.allocate(16 + e.data().length);
                body.putLong(e.index()).putLong(e.term()).put(e.data());
                CRC32C crc = new CRC32C();
                crc.update(body.array());
                ByteBuffer record = ByteBuffer.allocate(HEADER + body.capacity());
                record.putInt(body.capacity()).putInt((int) crc.getValue()).put(body.array()).flip();
                offsets.add(end);
                while (record.hasRemaining()) {
                    end += log.write(record, end);
                }
            }
            log.force(false);
        }
        if (state != null) {
            // Term and vote must be durable before any message that depends on them is sent. The commit
            // index need not be (the paper keeps it in memory): a stale one on disk only means a restarted
            // node re-learns it from the leader. So a change of commit alone is written lazily.
            if (written == null || state.term() != written.term() || state.votedFor() != written.votedFor()) {
                writeState(state);
            } else {
                lazyCommit = state;
            }
        }
    }

    /** Writes a commit index that only changed lazily; called from time to time and on close. */
    void flushCommit() throws IOException {
        if (lazyCommit != null) {
            writeState(lazyCommit);
        }
    }

    private void writeState(HardState s) throws IOException {
        written = s;
        lazyCommit = null;
        ByteBuffer b = ByteBuffer.allocate(24);
        b.putLong(s.term()).putInt(s.votedFor()).putLong(s.commit());
        CRC32C crc = new CRC32C();
        crc.update(b.array(), 0, 20);
        b.putInt((int) crc.getValue()).flip();
        Path tmp = dir.resolve("raft.state.tmp");
        try (FileChannel f = FileChannel.open(tmp, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {
            while (b.hasRemaining()) {
                f.write(b);
            }
            f.force(true);
        }
        Files.move(tmp, dir.resolve("raft.state"), StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        syncDirectory();
    }

    /** Makes the rename durable. Not possible on every OS; where it is not, the rename is best effort. */
    private void syncDirectory() {
        try (FileChannel d = FileChannel.open(dir, StandardOpenOption.READ)) {
            d.force(true);
        } catch (IOException | UnsupportedOperationException e) {
            // Windows cannot open a directory as a channel.
        }
    }

    @Override
    public void close() throws IOException {
        flushCommit();
        log.close();
    }
}
