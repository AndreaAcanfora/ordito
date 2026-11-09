package io.github.andreaacanfora.ordito.server;

import java.util.ArrayList;
import java.util.List;

/** A cluster member: its id, where clients reach it, and where other nodes reach it. */
public record Member(int id, String host, int clientPort, int raftPort) {

    /** Parses {@code 1=127.0.0.1:6381:7001,2=...}: id=host:clientPort:raftPort. */
    public static List<Member> parseCluster(String spec) {
        List<Member> out = new ArrayList<>();
        for (String part : spec.split(",")) {
            String[] idAndRest = part.trim().split("=", 2);
            if (idAndRest.length != 2) {
                throw new IllegalArgumentException("expected id=host:clientPort:raftPort, got '" + part + "'");
            }
            String[] f = idAndRest[1].split(":");
            if (f.length != 3) {
                throw new IllegalArgumentException("expected id=host:clientPort:raftPort, got '" + part + "'");
            }
            try {
                out.add(new Member(Integer.parseInt(idAndRest[0]), f[0], Integer.parseInt(f[1]), Integer.parseInt(f[2])));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("bad number in '" + part + "'");
            }
        }
        if (out.stream().map(Member::id).distinct().count() != out.size()) {
            throw new IllegalArgumentException("duplicate member id");
        }
        return List.copyOf(out);
    }

    public String clientAddress() {
        return host + ":" + clientPort;
    }

    @Override
    public String toString() {
        return id + "=" + host + ":" + clientPort + ":" + raftPort;
    }
}
