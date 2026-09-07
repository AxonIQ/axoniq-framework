package io.axoniq.shardlab;

import io.axoniq.framework.testcontainer.AxonServerContainer;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.jdbc.PostgresTokenTableFactory;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.jdbc.TokenSchema;
import org.testcontainers.containers.PostgreSQLContainer;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Shared infrastructure for the multi-process sharding lab: one Axon Server (DCB event store) and one PostgreSQL
 * (JDBC token store + the step-log side-effect table the tests use as an oracle).
 */
public final class Cluster implements AutoCloseable {

    /** Shared: only holds tokens and the step log, both of which are reset per test. */
    public static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>("postgres:16-alpine").withReuse(false);

    private static boolean postgresStarted;

    /**
     * One Axon Server per test. The event store must be empty at the start of every test: a node whose token store
     * is empty starts from the first event, so a shared event store would make each test replay the previous one.
     */
    private final AxonServerContainer axonServer =
            new AxonServerContainer("docker.axoniq.io/axoniq/axonserver:2025.2.7")
                    .withAxonServerHostname("localhost")
                    .withDevMode(true)
                    .withDcbContext(true);

    private final List<Node> nodes = new ArrayList<>();
    private final Path logDir;

    public Cluster(String name) {
        startPostgres();
        axonServer.start();
        try {
            this.logDir = Path.of("target", "shardlab", name);
            Files.createDirectories(logDir);
            resetTables();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static synchronized void startPostgres() {
        if (postgresStarted) {
            return;
        }
        POSTGRES.start();
        try (var connection = connect()) {
            try (var statement = PostgresTokenTableFactory.INSTANCE.createTable(connection, new TokenSchema())) {
                statement.executeUpdate();
            }
            try (var statement = connection.createStatement()) {
                statement.execute(StepLog.DDL);
                statement.execute(StepLog.OWNERSHIP_DDL);
            }
        } catch (Exception e) {
            throw new IllegalStateException("failed to create schema", e);
        }
        postgresStarted = true;
    }

    public static Connection connect() {
        try {
            return DriverManager.getConnection(POSTGRES.getJdbcUrl(),
                                               POSTGRES.getUsername(),
                                               POSTGRES.getPassword());
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private void resetTables() {
        try (var connection = connect(); var statement = connection.createStatement()) {
            statement.execute("TRUNCATE step_log");
            statement.execute("TRUNCATE ownership_log");
            statement.execute("DELETE FROM tokenentry");
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public String axonServerAddress() {
        return axonServer.getHost() + ":" + axonServer.getMappedPort(8124);
    }

    /** A publisher wired to this cluster's Axon Server. */
    public io.axoniq.shardlabdriver.Publisher publisher() {
        return new io.axoniq.shardlabdriver.Publisher(axonServerAddress(),
                                                      POSTGRES.getJdbcUrl(),
                                                      POSTGRES.getUsername(),
                                                      POSTGRES.getPassword());
    }

    /**
     * Starts a node as a separate OS process, so it can later be killed with SIGKILL without any shutdown hook
     * running and without the token claim being released.
     */
    public Node startNode(String nodeId, int segmentCount, String... extraArgs) {
        return startNode(nodeId, segmentCount, POSTGRES.getJdbcUrl(), extraArgs);
    }

    public Node startNode(String nodeId, int segmentCount, String jdbcUrl, String... extraArgs) {
        return startProcess(NodeApp.class.getName(), nodeId, segmentCount, jdbcUrl, extraArgs);
    }

    public Node startProcess(String mainClass, String nodeId, int segmentCount, String jdbcUrl,
                             String... extraArgs) {
        var java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        var command = new ArrayList<String>(List.of(
                java,
                "-cp", System.getProperty("java.class.path"),
                "-Dshardlab.node=" + nodeId,
                "-Dfile.encoding=UTF-8",
                mainClass,
                "--axon.axonserver.servers=" + axonServerAddress(),
                "--spring.datasource.url=" + jdbcUrl,
                "--spring.datasource.username=" + POSTGRES.getUsername(),
                "--spring.datasource.password=" + POSTGRES.getPassword(),
                "--axoniq.workflow.initial-segment-count=" + segmentCount
        ));
        command.addAll(List.of(extraArgs));
        try {
            var log = logDir.resolve(nodeId + "-" + System.currentTimeMillis() + ".log").toFile();
            var process = new ProcessBuilder(command)
                    .redirectErrorStream(true)
                    .redirectOutput(log)
                    .start();
            var node = new Node(nodeId, process, log);
            nodes.add(node);
            return node;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public List<Map<String, String>> query(String sql, Object... args) {
        try (var connection = connect(); var statement = connection.prepareStatement(sql)) {
            for (var i = 0; i < args.length; i++) {
                statement.setObject(i + 1, args[i]);
            }
            try (ResultSet rs = statement.executeQuery()) {
                var rows = new ArrayList<Map<String, String>>();
                var columns = rs.getMetaData().getColumnCount();
                while (rs.next()) {
                    var row = new LinkedHashMap<String, String>();
                    for (var c = 1; c <= columns; c++) {
                        row.put(rs.getMetaData().getColumnLabel(c), String.valueOf(rs.getObject(c)));
                    }
                    rows.add(row);
                }
                return rows;
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public List<Map<String, String>> steps() {
        return query("SELECT wf_id, step, node_id FROM step_log ORDER BY seq");
    }

    public List<Map<String, String>> tokens() {
        return query("SELECT segment, owner, timestamp FROM tokenentry WHERE processorname = 'Workflow' "
                             + "ORDER BY segment");
    }

    @Override
    public void close() {
        nodes.forEach(Node::kill);
        nodes.forEach(Node::awaitExit);
        axonServer.stop();
    }

    /** A node process. */
    public static final class Node {

        private final String id;
        private final Process process;
        private final File log;

        Node(String id, Process process, File log) {
            this.id = id;
            this.process = process;
            this.log = log;
        }

        public String id() {
            return id;
        }

        public long pid() {
            return process.pid();
        }

        public boolean alive() {
            return process.isAlive();
        }

        public String log() {
            try {
                return Files.readString(log.toPath());
            } catch (Exception e) {
                return "<unreadable: " + e + ">";
            }
        }

        /** SIGKILL: no shutdown hook, no claim release. A real crash. */
        public void kill() {
            process.destroyForcibly();
        }

        /** SIGTERM: Spring shutdown hooks run, claims are released cleanly. */
        public void stopGracefully() {
            process.destroy();
        }

        public int awaitExit() {
            try {
                process.waitFor(30, TimeUnit.SECONDS);
                return process.exitValue();
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }

        public void awaitReady(Duration timeout) {
            var deadline = System.currentTimeMillis() + timeout.toMillis();
            while (System.currentTimeMillis() < deadline) {
                if (log().contains("NODE_READY node=" + id)) {
                    return;
                }
                if (!process.isAlive()) {
                    throw new IllegalStateException("node " + id + " died before becoming ready:\n" + log());
                }
                sleep(200);
            }
            throw new IllegalStateException("node " + id + " never became ready:\n" + log());
        }
    }

    public static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
