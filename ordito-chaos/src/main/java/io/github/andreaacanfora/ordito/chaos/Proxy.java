package io.github.andreaacanfora.ordito.chaos;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A TCP proxy for one direction of one link between two nodes. Cutting it drops every connection and
 * refuses new ones until it is healed: to the nodes it looks like the network between them failed.
 */
final class Proxy implements Closeable {

    private final ServerSocket listener;
    private final InetSocketAddress target;
    private final Set<Socket> sockets = ConcurrentHashMap.newKeySet();
    private volatile boolean cut;
    private volatile boolean running = true;

    Proxy(int port, InetSocketAddress target) throws IOException {
        this.target = target;
        this.listener = new ServerSocket();
        listener.setReuseAddress(true);
        listener.bind(new InetSocketAddress("127.0.0.1", port));
        Thread.ofVirtual().start(this::acceptLoop);
    }

    int port() {
        return listener.getLocalPort();
    }

    void cut() {
        cut = true;
        closeAll();
    }

    void heal() {
        cut = false;
    }

    boolean isCut() {
        return cut;
    }

    private void acceptLoop() {
        while (running) {
            try {
                Socket in = listener.accept();
                if (cut) {
                    in.close();
                    continue;
                }
                Socket out = new Socket();
                try {
                    out.connect(target, 1000);
                } catch (IOException e) {
                    in.close();
                    continue;
                }
                in.setTcpNoDelay(true);
                out.setTcpNoDelay(true);
                sockets.add(in);
                sockets.add(out);
                Thread.ofVirtual().start(() -> pump(in, out));
                Thread.ofVirtual().start(() -> pump(out, in));
            } catch (IOException e) {
                // listener closed
            }
        }
    }

    private void pump(Socket from, Socket to) {
        byte[] buffer = new byte[64 * 1024];
        try (InputStream in = from.getInputStream(); OutputStream out = to.getOutputStream()) {
            int n;
            while ((n = in.read(buffer)) > 0) {
                if (cut) {
                    break;
                }
                out.write(buffer, 0, n);
            }
        } catch (IOException e) {
            // either side closed
        } finally {
            close(from);
            close(to);
        }
    }

    private void closeAll() {
        for (Socket s : sockets) {
            close(s);
        }
    }

    private void close(Socket s) {
        sockets.remove(s);
        try {
            s.close();
        } catch (IOException ignored) {
            // closing anyway
        }
    }

    @Override
    public void close() throws IOException {
        running = false;
        listener.close();
        closeAll();
    }
}
