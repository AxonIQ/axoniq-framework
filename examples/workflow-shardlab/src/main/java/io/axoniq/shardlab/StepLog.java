package io.axoniq.shardlab;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/**
 * Side-effect table. Every workflow step writes here with the id of the node that ran it, so the tests can see
 * which node executed which step of which workflow instance, and in what order.
 */
@Component
public class StepLog {

    public static final String DDL = """
            CREATE TABLE IF NOT EXISTS step_log (
                seq        BIGSERIAL PRIMARY KEY,
                wf_id      VARCHAR(255) NOT NULL,
                step       VARCHAR(255) NOT NULL,
                node_id    VARCHAR(255) NOT NULL,
                at         TIMESTAMP NOT NULL DEFAULT now()
            );
            """;

    public static final String OWNERSHIP_DDL = """
            CREATE TABLE IF NOT EXISTS ownership_log (
                seq      BIGSERIAL PRIMARY KEY,
                node_id  VARCHAR(255) NOT NULL,
                segments VARCHAR(1024) NOT NULL,
                at       TIMESTAMP NOT NULL DEFAULT now()
            );
            """;

    private final JdbcTemplate jdbc;
    private final String nodeId = System.getProperty("shardlab.node", "?");

    public StepLog(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public void record(String workflowId, String step) {
        jdbc.update("INSERT INTO step_log (wf_id, step, node_id) VALUES (?, ?, ?)", workflowId, step, nodeId);
    }

    public void recordOwnership(String node, String segments) {
        jdbc.update("INSERT INTO ownership_log (node_id, segments) VALUES (?, ?)", node, segments);
    }

    public String nodeId() {
        return nodeId;
    }

    /** Number of flaky attempts already recorded for this workflow, across all nodes. */
    public int countAttempts(String workflowId) {
        var count = jdbc.queryForObject(
                "SELECT count(*) FROM step_log WHERE wf_id = ? AND step LIKE 'flakyStep-attempt-%'",
                Integer.class, workflowId);
        return count == null ? 0 : count;
    }
}
