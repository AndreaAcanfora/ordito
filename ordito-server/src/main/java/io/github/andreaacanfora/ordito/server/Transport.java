package io.github.andreaacanfora.ordito.server;

import io.github.andreaacanfora.ordito.raft.Message;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Messages between nodes over TCP, as length-prefixed frames. Each peer gets one outgoing connection,
 * fed by a queue and a virtual thread that reconnects when the link breaks. Raft tolerates lost
 * messages, so when a peer is unreachable or slow its messages are dropped instead of piling up.
 */
final class Transport implements Closeable {

    private static final int QUEUE = 4096;
    private static final int MAX_FRAME = 256 * 1024 * 1024;

    private final ServerSocket listener;
    private final Consumer<Message> inbound;
    private final Map<Integer, Peer> peers = new ConcurrentHashMap<>();
    private final Set<Socket> open = ConcurrentHashMap.newKeySet();
    private final List<Thread> senders = new CopyOnWriteArrayList<>();
    private volatile boolean running = true;

    Transport(InetSocketAddress bind, Map<Integer, InetSocketAddress> peerAddresses, Consumer<Message> inbound)
            throws IOException {
        this.inbound = inbound;
        this.listener = new ServerSocket();
        listener.setReuseAddress(true);
        listener.bind(bind, 128);
        Thread.ofVirtual().name("raft-accept").start(this::acceptLoop);
        peerAddresses.forEach((id, address) -> {
            Peer peer = new Peer(address);
            peers.put(id, peer);
            senders.add(Thread.ofVirtual().name("raft-send-" + id).start(peer::sendLoop));
        });
    }

    int port() {
        return listener.getLocalPort();
    }

    void send(List<Message> messages) {
        for (Message m : messages) {
            Peer peer = peers.get(m.to());
            if (peer != null) {
                peer.queue.offer(m); // full: drop it, Raft will retry
            }
        }
    }

    private void acceptLoop() {
        while (running) {
            try {
                Socket socket = listener.accept();
                socket.setTcpNoDelay(true);
                open.add(socket);
                Thread.ofVirtual().name("raft-recv").start(() -> receiveLoop(socket));
            } catch (IOException e) {
                if (running) {
                    System.err.println("ordito: raft accept failed: " + e.getMessage());
                }
            }
        }
    }

    private void receiveLoop(Socket socket) {
        try (socket; var in = new DataInputStream(new BufferedInputStream(socket.getInputStream(), 64 * 1024))) {
            while (running) {
                int length = in.readInt();
                if (length <= 0 || length > MAX_FRAME) {
                    return;
                }
                inbound.accept(Codec.decode(in.readNBytes(length)));
            }
        } catch (IOException e) {
            // The peer went away; it will reconnect.
        } finally {
            open.remove(socket);
        }
    }

    private final class Peer {
        final InetSocketAddress address;
        final BlockingQueue<Message> queue = new ArrayBlockingQueue<>(QUEUE);

        Peer(InetSocketAddress address) {
            this.address = address;
        }

        void sendLoop() {
            while (running) {
                try (Socket socket = new Socket()) {
                    open.add(socket);
                    socket.connect(address, 1000);
                    socket.setTcpNoDelay(true);
                    var out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream(), 64 * 1024));
                    while (running) {
                        Message m = queue.poll(1, TimeUnit.SECONDS);
                        if (m == null) {
                            continue;
                        }
                        write(out, m);
                        Message more;
                        while ((more = queue.poll()) != null) {
                            write(out, more);
                        }
                        out.flush();
                    }
                } catch (IOException e) {
                    // Unreachable or reset: drop what is queued (it is stale by now) and try again shortly.
                    queue.clear();
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException ie) {
                        return;
                    }
                } catch (InterruptedException e) {
                    return;
                } finally {
                    open.removeIf(Socket::isClosed);
                }
            }
        }

        private void write(DataOutputStream out, Message m) throws IOException {
            byte[] frame = Codec.encode(m);
            out.writeInt(frame.length);
            out.write(frame);
        }
    }

    @Override
    public void close() throws IOException {
        running = false;
        listener.close();
        senders.forEach(Thread::interrupt);
        for (Socket socket : open) {
            try {
                socket.close();
            } catch (IOException ignored) {
                // closing anyway
            }
        }
    }
}
