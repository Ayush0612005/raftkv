package io.raftkv.server;

import io.raftkv.cli.Main;
import io.raftkv.client.RaftKvClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/** End to end over real gRPC and real WALs: three server processes' worth of nodes on localhost. */
class GrpcClusterTest {

    @TempDir
    Path dataDir;

    private final Map<Integer, String> cluster = new LinkedHashMap<>();
    private final RaftKvServer[] servers = new RaftKvServer[3];
    private int snapshotThreshold = io.raftkv.raft.RaftConfig.DEFAULT_SNAPSHOT_THRESHOLD;

    private void startCluster() throws IOException {
        for (int i = 0; i < 3; i++) {
            cluster.put(i + 1, "localhost:" + freePort());
        }
        for (int i = 0; i < 3; i++) {
            startServer(i);
        }
    }

    private void startServer(int i) {
        servers[i] = RaftKvServer.start(new RaftKvServer.Options(
                i + 1, cluster, dataDir.resolve("node-" + (i + 1)), true, snapshotThreshold));
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private int leaderIndex() throws InterruptedException {
        for (int attempt = 0; attempt < 100; attempt++) {
            for (int i = 0; i < 3; i++) {
                if (servers[i] != null && servers[i].status().isLeader()) {
                    return i;
                }
            }
            Thread.sleep(50);
        }
        fail("no leader elected over gRPC");
        return -1;
    }

    @AfterEach
    void stop() {
        for (RaftKvServer server : servers) {
            if (server != null) {
                server.close();
            }
        }
    }

    @Test
    void putGetDeleteSurviveLeaderCrashAndRestart() throws Exception {
        startCluster();
        try (RaftKvClient client = RaftKvClient.connect(cluster)) {
            assertFalse(client.put("lang", "java"));
            assertEquals(Optional.of("java"), client.get("lang"));
            for (int i = 0; i < 50; i++) {
                client.put("key-" + i, "value-" + i);
            }

            // kill the leader: the client finds the new one and nothing committed is lost
            int leader = leaderIndex();
            servers[leader].close();
            servers[leader] = null;
            assertEquals(Optional.of("java"), client.get("lang"));
            assertTrue(client.put("lang", "java-25"));
            assertTrue(client.delete("key-0"));
            assertFalse(client.delete("key-0"));

            // restart the old leader from its WAL: it catches up with what it missed
            startServer(leader);
            long target = servers[(leader + 1) % 3].status().commitIndex();
            for (int attempt = 0; attempt < 100 && servers[leader].status().lastApplied() < target; attempt++) {
                Thread.sleep(50);
            }
            assertTrue(servers[leader].status().lastApplied() >= target, "restarted node did not catch up");
            assertEquals("java-25", servers[leader].stateMachine().peek("lang"));
            assertEquals(null, servers[leader].stateMachine().peek("key-0"));
            assertEquals("value-49", servers[leader].stateMachine().peek("key-49"));
        }
    }

    /** InstallSnapshot over real gRPC: a follower that was down while the others compacted their logs. */
    @Test
    void restartedFollowerReceivesSnapshotOverGrpc() throws Exception {
        snapshotThreshold = 20;
        startCluster();
        try (RaftKvClient client = RaftKvClient.connect(cluster)) {
            client.put("first", "1");
            int follower = (leaderIndex() + 1) % 3;
            servers[follower].close();
            servers[follower] = null;
            for (int i = 0; i < 200; i++) {
                client.put("k" + i, "v" + i);
            }
            int leader = leaderIndex();
            assertTrue(servers[leader].status().snapshotIndex() > 100, "leader should have compacted its log");

            startServer(follower);
            long target = servers[leader].status().commitIndex();
            for (int attempt = 0; attempt < 100 && servers[follower].status().lastApplied() < target; attempt++) {
                Thread.sleep(50);
            }
            assertTrue(servers[follower].status().lastApplied() >= target, "follower did not catch up");
            assertTrue(servers[follower].status().snapshotIndex() > 100, "catch-up should have come from a snapshot");
            assertEquals("1", servers[follower].stateMachine().peek("first"));
            assertEquals("v199", servers[follower].stateMachine().peek("k199"));
        }
    }

    @Test
    void commandLineClient() throws Exception {
        startCluster();
        String spec = cluster.entrySet().stream()
                .map(e -> e.getKey() + "=" + e.getValue())
                .collect(Collectors.joining(","));
        CommandLine cli = new CommandLine(new Main());
        assertEquals(0, cli.execute("put", "-c", spec, "city", "Munich"));
        assertEquals(0, cli.execute("get", "-c", spec, "city"));
        assertEquals(0, cli.execute("status", "-c", spec));
        assertEquals(0, cli.execute("delete", "-c", spec, "city"));
        assertEquals(1, cli.execute("get", "-c", spec, "city"), "absent key exits with 1");
    }
}
