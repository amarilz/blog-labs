package com.amarildoaliaj;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

class CassandraReadRepairIT {

    private static final String KS = "rr_demo";
    private static final String TABLE = "kv";
    private static final InetSocketAddress NODE1 = new InetSocketAddress("127.0.0.1", 9042);
    private static final InetSocketAddress NODE2 = new InetSocketAddress("127.0.0.1", 9043);
    private static final InetSocketAddress NODE3 = new InetSocketAddress("127.0.0.1", 9044);

    /**
     * Directory where docker-compose.yml is located.
     * Set to "." if running tests from the project root containing the compose file.
     */
    private static final File COMPOSE_DIR = new File(".");

    private CqlSession session;

    private PreparedStatement insertPs;
    private PreparedStatement selectPs;

    @BeforeEach
    void setUp() {

        session = sessionToAll();
        waitForNodesUp(session, 3, Duration.ofSeconds(90));
        setupSchema(session);

        insertPs = session.prepare("INSERT INTO " + KS + "." + TABLE + " (k, v) VALUES (?, ?)");
        selectPs = session.prepare("SELECT v FROM " + KS + "." + TABLE + " WHERE k=?");
    }

    @AfterEach
    void tearDown() {

        if (session != null) {
            session.close();
        }
        // best effort: bring cassandra3 back up if a test is interrupted midway
        try {
            startNode("cassandra3");
        } catch (Exception ignored) {
        }
    }

    @Test
    @DisplayName("Read repair: QUORUM read repairs stale replica (node3)")
    void readRepairQuorumRepairsNode3() throws Exception {

        String key = "user:42";

        // 1) Ensure all nodes are consistent
        writeValue(key, "A", ConsistencyLevel.ALL);

        // 2) Stop node3 and wait until it is effectively down
        stopNode("cassandra3");
        awaitNodeDown(NODE3, "dc1", Duration.ofSeconds(60));

        // 3) Write using QUORUM (node1+node2), leaving node3 stale
        writeValue(key, "B", ConsistencyLevel.QUORUM);

        // 4) Bring node3 back up and wait until it is reachable
        startNode("cassandra3");
        awaitNodeUp(NODE3, "dc1", Duration.ofSeconds(90));

        // 5) Read directly from node3 (CL=ONE on a session talking only to node3)
        String before = readFromSingleNode(NODE3, key, ConsistencyLevel.ONE);

        // "before" might already be "B" due to hints or background repair; we log it and don't fail here
        System.out.println("Before QUORUM read: node3@CL=ONE = " + before);

        // 6) Perform a QUORUM read to trigger digest mismatch and read repair
        String quorumRead = readValue(key, ConsistencyLevel.QUORUM);
        assertEquals("B", quorumRead, "QUORUM must return the latest value");

        // 7) Wait until node3 sees "B" (read repair can be asynchronous)
        awaitEventually(
                () -> "B".equals(readFromSingleNode(NODE3, key, ConsistencyLevel.ONE)),
                Duration.ofSeconds(30),
                Duration.ofMillis(500),
                "Expected node3 to be repaired to value B");

        String after = readFromSingleNode(NODE3, key, ConsistencyLevel.ONE);
        System.out.println("After QUORUM read: node3@CL=ONE = " + after);
        assertEquals("B", after);
    }

    // -----------------------------
    // Cassandra helpers
    // -----------------------------

    private void setupSchema(@NotNull CqlSession s) {

        s.execute("CREATE KEYSPACE IF NOT EXISTS " + KS +
                  " WITH replication = {'class':'NetworkTopologyStrategy','dc1':'3'}");
        s.execute("CREATE TABLE IF NOT EXISTS " + KS + "." + TABLE + " (" +
                  "k text PRIMARY KEY, v text)");

        // Note: In Cassandra 5, many "read_repair_chance" options are removed/deprecated
        // Read-repair on digest mismatch is still available
        // If this ALTER fails, we ignore it
        try {
            s.execute("ALTER TABLE " + KS + "." + TABLE +
                      " WITH read_repair_chance = 1.0 AND dclocal_read_repair_chance = 1.0");
        } catch (Exception ignored) {
        }
    }

    private void writeValue(String key, String value, ConsistencyLevel cl) {

        BoundStatement bs = insertPs.bind(key, value).setConsistencyLevel(cl);
        session.execute(bs);
    }

    @Nullable
    private String readValue(String key, ConsistencyLevel cl) {

        BoundStatement bs = selectPs.bind(key).setConsistencyLevel(cl);
        Row row = session.execute(bs).one();
        return row == null ? null : row.getString("v");
    }

    /**
     * Ad-hoc session to a single node, useful for verifying the "real" state of that specific replica
     */
    @Nullable
    private String readFromSingleNode(InetSocketAddress node, String key, ConsistencyLevel cl) {

        try (CqlSession s = sessionTo(node)) {
            PreparedStatement ps = s.prepare("SELECT v FROM " + KS + "." + TABLE + " WHERE k=?");
            BoundStatement bs = ps.bind(key).setConsistencyLevel(cl);
            Row row = s.execute(bs).one();
            return row == null ? null : row.getString("v");
        }
    }

    // -----------------------------
    // Driver sessions
    // -----------------------------

    @NotNull
    private static CqlSession sessionToAll() {

        ProgrammaticDriverConfigLoaderBuilder loader = DriverConfigLoader.programmaticBuilder()
                                                                         .withDuration(
                                                                                 DefaultDriverOption.REQUEST_TIMEOUT,
                                                                                 Duration.ofSeconds(10));

        return CqlSession.builder()
                         .addContactPoint(NODE1)
                         .addContactPoint(NODE2)
                         .addContactPoint(NODE3)
                         .withLocalDatacenter("dc1")
                         .withConfigLoader(loader.build())
                         .build();
    }

    private static @NotNull CqlSession sessionTo(InetSocketAddress node) {

        ProgrammaticDriverConfigLoaderBuilder loader = DriverConfigLoader.programmaticBuilder()
                                                                         .withDuration(
                                                                                 DefaultDriverOption.REQUEST_TIMEOUT,
                                                                                 Duration.ofSeconds(5));

        return CqlSession.builder()
                         .addContactPoint(node)
                         .withLocalDatacenter("dc1")
                         .withConfigLoader(loader.build())
                         .build();
    }

    private static void waitForNodesUp(CqlSession session, int requiredUp, Duration timeout) {

        awaitEventually(
                () -> session.getMetadata()
                             .getNodes()
                             .values()
                             .stream()
                             .filter(n -> n.getState() == NodeState.UP)
                             .count() >= requiredUp,
                timeout,
                Duration.ofMillis(500),
                "Cluster not ready: expected at least " + requiredUp + " nodes UP");
    }

    // -----------------------------
    // Docker compose helpers
    // -----------------------------

    private static void stopNode(String serviceName) throws Exception {

        runCompose("stop", serviceName);
    }

    private static void startNode(String serviceName) throws Exception {

        runCompose("start", serviceName);
    }

    private static void runCompose(String @NotNull ... args) throws Exception {
        // Compatible with "docker compose" (V2 plugin). If using "docker-compose" (V1), change here.
        String[] cmd = new String[2 + args.length];
        cmd[0] = "docker";
        cmd[1] = "compose";
        System.arraycopy(args, 0, cmd, 2, args.length);

        ProcessBuilder pb = new ProcessBuilder(cmd)
                .directory(COMPOSE_DIR)
                .redirectErrorStream(true);

        Process p = pb.start();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        p.getInputStream().transferTo(baos);

        boolean ok = p.waitFor(120, TimeUnit.SECONDS);
        if (!ok) {
            p.destroyForcibly();
            throw new IllegalStateException("docker compose command timed out: " + String.join(" ", cmd));
        }
        if (p.exitValue() != 0) {
            String out = baos.toString(StandardCharsets.UTF_8);
            throw new IllegalStateException(
                    "docker compose failed (" + p.exitValue() + "): " + String.join(" ", cmd) + "\n" + out);
        }
    }

    private static void awaitNodeDown(InetSocketAddress node, String dc, Duration timeout) {

        awaitEventually(() -> !canConnect(node, dc), timeout, Duration.ofMillis(500),
                        "Expected node " + node + " to be DOWN");
    }

    private static void awaitNodeUp(InetSocketAddress node, String dc, Duration timeout) {

        awaitEventually(() -> canConnect(node, dc), timeout, Duration.ofMillis(500),
                        "Expected node " + node + " to be UP");
    }

    private static boolean canConnect(InetSocketAddress node, String dc) {

        try (CqlSession s = CqlSession.builder()
                                      .addContactPoint(node)
                                      .withLocalDatacenter(dc)
                                      .build()) {
            s.execute("SELECT now() FROM system.local");
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    // -----------------------------
    // Generic await helper (no busy-wait)
    // -----------------------------

    private static void awaitEventually(BooleanSupplier condition,
                                        @NotNull Duration timeout,
                                        Duration pollInterval,
                                        String failureMessage) {

        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(pollInterval.toMillis());
            } catch (InterruptedException ignored) {
            }
        }
        fail(failureMessage);
    }
}
