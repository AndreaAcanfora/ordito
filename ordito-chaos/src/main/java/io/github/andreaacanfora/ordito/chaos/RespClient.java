package io.github.andreaacanfora.ordito.chaos;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

/** A tiny blocking RESP client: one command, one reply. */
final class RespClient implements Closeable {

    /** A reply: {@code kind} is the RESP type byte; {@code text} the line or the bulk string (null for nil). */
    record Reply(char kind, String text) {
        boolean isError() {
            return kind == '-';
        }
    }

    private final Socket socket;
    private final InputStream in;
    private final OutputStream out;

    RespClient(String host, int port, int timeoutMs) throws IOException {
        socket = new Socket();
        socket.connect(new InetSocketAddress(host, port), timeoutMs);
        socket.setSoTimeout(timeoutMs);
        socket.setTcpNoDelay(true);
        in = new BufferedInputStream(socket.getInputStream());
        out = new BufferedOutputStream(socket.getOutputStream());
    }

    Reply call(String... args) throws IOException {
        StringBuilder b = new StringBuilder().append('*').append(args.length).append("\r\n");
        for (String a : args) {
            b.append('$').append(a.getBytes(StandardCharsets.UTF_8).length).append("\r\n").append(a).append("\r\n");
        }
        out.write(b.toString().getBytes(StandardCharsets.UTF_8));
        out.flush();
        String line = line();
        char kind = line.charAt(0);
        String rest = line.substring(1);
        if (kind == '$') {
            int n = Integer.parseInt(rest);
            if (n < 0) {
                return new Reply('$', null);
            }
            String s = new String(in.readNBytes(n), StandardCharsets.UTF_8);
            line();
            return new Reply('$', s);
        }
        return new Reply(kind, rest);
    }

    private String line() throws IOException {
        StringBuilder s = new StringBuilder();
        int c;
        while ((c = in.read()) != '\r') {
            if (c < 0) {
                throw new IOException("connection closed");
            }
            s.append((char) c);
        }
        if (in.read() != '\n') {
            throw new IOException("bad line ending");
        }
        return s.toString();
    }

    @Override
    public void close() {
        try {
            socket.close();
        } catch (IOException ignored) {
            // closing anyway
        }
    }
}
