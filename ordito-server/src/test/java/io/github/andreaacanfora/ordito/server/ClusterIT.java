package io.github.andreaacanfora.ordito.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.andreaacanfora.ordito.raft.RaftNode.Role;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Three real nodes in one JVM, over real sockets and real disks. */
class ClusterIT {

    @TempDir
    Path tmp;

    private List<Member> cluster;
    private final ReplicatedServer[] servers = new ReplicatedServer[4];

    private void startCluster() throws IOException {
        cluster = new ArrayList<>();
        for (int id = 1; id <= 3; id++) {
            cluster.add(new Member(id, "127.0.0.1", freePort(), freePort()));
        }
        for (int id = 1; id <= 3; id++) {
            start(id);
        }
    }

    private void start(int id) throws IOException {
        servers[id] = new ReplicatedServer(new ReplicatedServer.Options(id, cluster, tmp.resolve("n" + id),
                "127.0.0.1", 10, Map.of(), false)).start();
    }

    private void stop(int id) throws IOException {
        servers[id].close();
        servers[id] = null;
    }

    @AfterEach
    void stopAll() throws IOException {
        for (int id = 1; id <= 3; id++) {
            if (servers[id] != null) {
                stop(id);
            }
        }
    }

    private int leader() throws InterruptedException {
        for (int i = 0; i < 300; i++) {
            for (int id = 1; id <= 3; id++) {
                if (servers[id] != null && servers[id].status().role() == Role.LEADER) {
                    return id;
                }
            }
            Thread.sleep(20);
        }
        throw new AssertionError("no leader elected");
    }

    @Test
    void writesAreReplicatedAndFollowersRedirect() throws Exception {
        startCluster();
        int leader = leader();
        try (Client c = new Client(cluster.get(leader - 1).clientPort())) {
            assertEquals("+OK", c.call("SET", "greeting", "ciao"));
            assertEquals("ciao", c.call("GET", "greeting"));
            assertEquals(":1", c.call("RPUSH", "list", "a"));
            assertEquals(":2", c.call("RPUSH", "list", "b"));
        }
        int follower = leader % 3 + 1;
        try (Client c = new Client(cluster.get(follower - 1).clientPort())) {
            assertEquals("-MOVED 0 " + cluster.get(leader - 1).clientAddress(), c.call("GET", "greeting"));
            assertEquals("+PONG", c.call("PING"));
            assertTrue(c.call("RAFT", "STATUS").contains("role:follower"));
        }
    }

    @Test
    void theClusterSurvivesTheLeaderAndARestartCatchesUp() throws Exception {
        startCluster();
        int leader = leader();
        try (Client c = new Client(cluster.get(leader - 1).clientPort())) {
            for (int i = 0; i < 100; i++) {
                assertEquals("+OK", c.call("SET", "k" + i, "v" + i));
            }
        }
        stop(leader);
        int next = leader();
        try (Client c = new Client(cluster.get(next - 1).clientPort())) {
            assertEquals("v42", c.call("GET", "k42"));
            assertEquals("+OK", c.call("SET", "after", "failover"));
        }
        start(leader); // restarts from its own disk and catches up from the new leader
        for (int i = 0; i < 300 && servers[leader].status().applied() < servers[next].status().commit(); i++) {
            Thread.sleep(20);
        }
        assertEquals(servers[next].status().commit(), servers[leader].status().applied());
    }

    @Test
    void expiryIsDrivenByTheReplicatedClock() throws Exception {
        startCluster();
        int leader = leader();
        try (Client c = new Client(cluster.get(leader - 1).clientPort())) {
            assertEquals("+OK", c.call("SET", "session", "x", "PX", "300"));
            assertEquals("x", c.call("GET", "session"));
            Thread.sleep(700);
            assertEquals("(nil)", c.call("GET", "session"));
        }
    }

    @Test
    void nothingIsLostAcrossAFullRestart() throws Exception {
        startCluster();
        int leader = leader();
        try (Client c = new Client(cluster.get(leader - 1).clientPort())) {
            assertEquals(":5", c.call("INCRBY", "counter", "5"));
        }
        for (int id = 1; id <= 3; id++) {
            stop(id);
        }
        for (int id = 1; id <= 3; id++) {
            start(id);
        }
        leader = leader();
        try (Client c = new Client(cluster.get(leader - 1).clientPort())) {
            assertEquals(":6", c.call("INCR", "counter"));
        }
    }

    // ---- a minimal RESP client ----

    static final class Client implements AutoCloseable {
        private final Socket socket;
        private final InputStream in;
        private final OutputStream out;

        Client(int port) throws IOException {
            socket = new Socket("127.0.0.1", port);
            socket.setSoTimeout(10_000);
            in = new BufferedInputStream(socket.getInputStream());
            out = socket.getOutputStream();
        }

        /** Sends a command; returns status, error and integer replies as their raw line, bulk strings as text. */
        String call(String... args) throws IOException {
            StringBuilder b = new StringBuilder("*" + args.length + "\r\n");
            for (String a : args) {
                b.append('$').append(a.getBytes(StandardCharsets.UTF_8).length).append("\r\n").append(a).append("\r\n");
            }
            out.write(b.toString().getBytes(StandardCharsets.UTF_8));
            out.flush();
            String line = line();
            if (line.startsWith("$")) {
                int n = Integer.parseInt(line.substring(1));
                if (n < 0) {
                    return "(nil)";
                }
                String s = new String(in.readNBytes(n), StandardCharsets.UTF_8);
                line();
                return s;
            }
            return line;
        }

        private String line() throws IOException {
            StringBuilder s = new StringBuilder();
            int c;
            while ((c = in.read()) != '\r') {
                if (c < 0) {
                    throw new IOException("closed");
                }
                s.append((char) c);
            }
            in.read();
            return s.toString();
        }

        @Override
        public void close() throws IOException {
            socket.close();
        }
    }

    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }
}
