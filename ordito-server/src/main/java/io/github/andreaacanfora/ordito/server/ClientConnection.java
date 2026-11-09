package io.github.andreaacanfora.ordito.server;

import io.github.andreaacanfora.loomkv.resp.ProtocolException;
import io.github.andreaacanfora.loomkv.resp.RespReader;
import io.github.andreaacanfora.loomkv.resp.RespValue;
import io.github.andreaacanfora.loomkv.resp.RespWriter;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/** One client connection on its own virtual thread, speaking RESP with loomkv's reader and writer. */
final class ClientConnection implements Runnable {

    /** Answered by the node itself: they do not read or change the data. */
    private static final Set<String> LOCAL = Set.of("PING", "ECHO", "INFO", "COMMAND", "CONFIG", "SELECT", "TIME");

    private final ReplicatedServer server;
    private final Socket socket;
    private String name = "";

    ClientConnection(ReplicatedServer server, Socket socket) {
        this.server = server;
        this.socket = socket;
    }

    @Override
    public void run() {
        try (socket) {
            RespReader reader = new RespReader(new BufferedInputStream(socket.getInputStream(), 16 * 1024));
            RespWriter writer = new RespWriter(new BufferedOutputStream(socket.getOutputStream(), 16 * 1024));
            while (true) {
                List<byte[]> command;
                try {
                    command = reader.readCommand();
                } catch (ProtocolException e) {
                    writer.write(RespValue.error("ERR " + e.getMessage()));
                    writer.flush();
                    return;
                }
                if (command == null) {
                    return;
                }
                if (command.isEmpty()) {
                    continue;
                }
                String cmd = new String(command.getFirst(), StandardCharsets.UTF_8).toUpperCase(Locale.ROOT);
                if (cmd.equals("QUIT")) {
                    writer.write(RespValue.OK);
                    writer.flush();
                    return;
                }
                writer.write(handle(cmd, command));
                if (!reader.hasBuffered()) {
                    writer.flush();
                }
            }
        } catch (IOException e) {
            // client went away
        }
    }

    private RespValue handle(String cmd, List<byte[]> command) {
        if (LOCAL.contains(cmd)) {
            return server.local(command);
        }
        return switch (cmd) {
            case "RAFT" -> raft(command);
            case "CLIENT" -> client(command);
            case "SUBSCRIBE", "PSUBSCRIBE", "UNSUBSCRIBE", "PUNSUBSCRIBE" ->
                    RespValue.error("ERR pub/sub is not supported by the replicated server");
            default -> server.handle(command);
        };
    }

    private RespValue raft(List<byte[]> command) {
        String sub = command.size() > 1 ? new String(command.get(1), StandardCharsets.UTF_8).toUpperCase(Locale.ROOT) : "";
        if (!sub.equals("STATUS")) {
            return RespValue.error("ERR usage: RAFT STATUS");
        }
        return RespValue.bulk(server.statusText());
    }

    private RespValue client(List<byte[]> command) {
        String sub = command.size() > 1 ? new String(command.get(1), StandardCharsets.UTF_8).toUpperCase(Locale.ROOT) : "";
        return switch (sub) {
            case "SETNAME" -> {
                if (command.size() != 3) {
                    yield RespValue.error("ERR wrong number of arguments for 'client|setname' command");
                }
                name = new String(command.get(2), StandardCharsets.UTF_8);
                yield RespValue.OK;
            }
            case "GETNAME" -> RespValue.bulk(name.isEmpty() ? null : name);
            case "SETINFO" -> RespValue.OK;
            default -> RespValue.error("ERR unknown subcommand '" + sub.toLowerCase(Locale.ROOT) + "'");
        };
    }

    void close() {
        try {
            socket.close();
        } catch (IOException ignored) {
            // already closed
        }
    }
}
