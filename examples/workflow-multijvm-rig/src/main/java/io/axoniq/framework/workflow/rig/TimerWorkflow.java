/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 *
 * The software is available for evaluation use without registration.
 * Continued use beyond the evaluation period requires registration
 * and a commercial license. See the License for the specific language
 * governing permissions and limitations under the License.
 * You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at:
 *  https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *  https://www.axoniq.io/pricing
 */
package io.axoniq.framework.workflow.rig;

import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.framework.workflow.runtime.api.annotation.Workflow;
import io.axoniq.framework.workflow.runtime.api.execution.context.retry.BackoffStrategy;
import io.axoniq.framework.workflow.runtime.api.execution.context.retry.RetryPolicy;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;

import static io.axoniq.framework.workflow.dsl.base.BaseWorkflowContext.equalsTo;
import static io.axoniq.framework.workflow.runtime.association.Associations.associate;
import static io.axoniq.framework.workflow.runtime.association.PayloadPropertyValueRetriever.payloadProperty;

/**
 * Two workflows whose only interesting state is a running countdown: one waiting out a step timeout, one sitting in
 * retry backoff.
 * <p>
 * Both countdowns live in an in-memory scheduler inside the node that armed them, while the fact that they were armed
 * lives in the event store. A segment moving between nodes therefore separates the two, and nothing about that
 * separation produces an error: a dropped countdown looks exactly like a workflow that is still patiently waiting.
 * <p>
 * Every observable moment is a row in {@code rig_step_log}, written from inside a step so that rehydrating the instance
 * replays it rather than repeating it. {@code armed} is written once the countdown is durably recorded, and the
 * settling row once it has run out, each tagged with the node that was holding the segment at the time.
 */
@Component
public class TimerWorkflow {

    /**
     * How long {@code waitWithTimeout} waits for an event that is never published.
     */
    public static final Duration WAIT_TIMEOUT = Duration.ofSeconds(60);

    /**
     * Pause between attempts of {@code flakyStep}. This is the window a segment handover has to land inside.
     */
    public static final Duration RETRY_BACKOFF = Duration.ofSeconds(30);

    /**
     * Retries after the initial attempt, so {@code RETRY_MAX + 1} attempts in total.
     */
    public static final int RETRY_MAX = 1;

    /**
     * Budget for the whole of {@code flakyStep}, backoff included. The DSL default is five seconds, which would time
     * the step out long before the retries are exhausted and hide whatever the backoff did.
     */
    public static final Duration RETRY_STEP_TIMEOUT = Duration.ofMinutes(4);

    private final JdbcTemplate jdbcTemplate;
    private final String nodeId;

    TimerWorkflow(JdbcTemplate jdbcTemplate, @Value("${rig.node-id}") String nodeId) {
        this.jdbcTemplate = jdbcTemplate;
        this.nodeId = nodeId;
    }

    /**
     * Records a wait with a timeout, then blocks on it. The awaited event is never published, so the only way past the
     * wait is the timeout firing.
     *
     * @param ctx the workflow context.
     */
    @Workflow(
            idProperty = "id",
            startOnEventClass = TimeoutArmed.class,
            workflowName = "TimeoutTimerWorkflow"
    )
    public void timeout(SimpleWorkflowContext ctx) {
        var wait = ctx.waitForEvent("waitWithTimeout",
                                    TimeoutReleased.class,
                                    associate(payloadProperty("id"), equalsTo(ctx.workflowPayload().get("id"))),
                                    step -> step.timeout(WAIT_TIMEOUT));
        ctx.awaitExecute("armTimeout", Map.of(), (pc, payload) -> log(ctx.workflowId(), "armed"));

        wait.await();
        var outcome = wait.timeout() ? "timedout" : "released";
        ctx.awaitExecute("settleTimeout", Map.of(), (pc, payload) -> log(ctx.workflowId(), outcome));
    }

    /**
     * Runs a step that always fails, under a retry policy with a fixed backoff, then records how it ended.
     *
     * @param ctx the workflow context.
     */
    @Workflow(
            idProperty = "id",
            startOnEventClass = RetryArmed.class,
            workflowName = "RetryTimerWorkflow"
    )
    public void retry(SimpleWorkflowContext ctx) {
        ctx.awaitExecute("armRetry", Map.of(), (pc, payload) -> log(ctx.workflowId(), "armed"));

        var flaky = ctx.execute("flakyStep",
                                Map.of(),
                                (pc, payload) -> {
                                    recordAttempt(ctx.workflowId());
                                    throw new IllegalStateException("flakyStep always fails");
                                },
                                step -> step.timeout(RETRY_STEP_TIMEOUT)
                                            .retryPolicy(RetryPolicy.maxRetries(RETRY_MAX)
                                                                    .withBackoff(BackoffStrategy.fixed(
                                                                            RETRY_BACKOFF))));

        flaky.await();
        var outcome = flaky.success() ? "succeeded" : "exhausted";
        ctx.awaitExecute("settleRetry", Map.of(), (pc, payload) -> log(ctx.workflowId(), outcome));
    }

    /**
     * Records one attempt of the failing step on a connection of its own.
     * <p>
     * A step body runs inside the transaction of the message being handled, and a body that throws rolls that
     * transaction back. Written through the usual template, the row of a failed attempt disappears along with the
     * failure, and the step log would then show a step that was retried as one that was never attempted at all. Taking
     * a connection straight from the pool puts the row outside that transaction, which is the only way a failed
     * attempt can leave a trace.
     */
    private void recordAttempt(String workflowId) {
        try (var connection = Objects.requireNonNull(jdbcTemplate.getDataSource()).getConnection();
             var statement = connection.prepareStatement(
                     "INSERT INTO rig_step_log (workflow_id, step, node_id, logged_at) VALUES (?, ?, ?, ?)")) {
            statement.setString(1, workflowId);
            statement.setString(2, "attempt");
            statement.setString(3, nodeId);
            statement.setLong(4, System.currentTimeMillis());
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("Could not record an attempt of " + workflowId, e);
        }
    }

    private Map<String, Object> log(String workflowId, String step) {
        jdbcTemplate.update(
                "INSERT INTO rig_step_log (workflow_id, step, node_id, logged_at) VALUES (?, ?, ?, ?)",
                workflowId, step, nodeId, System.currentTimeMillis()
        );
        return Map.of(step + "By", nodeId);
    }

    /**
     * Starts an instance whose wait carries a timeout.
     *
     * @param id the workflow id, which also decides the owning segment.
     */
    public record TimeoutArmed(String id) {

    }

    /**
     * Satisfies the wait of a {@link TimeoutArmed} instance. The scenarios never publish it; it exists so the wait has
     * something to be waiting for.
     *
     * @param id the workflow id whose wait it satisfies.
     */
    public record TimeoutReleased(String id) {

    }

    /**
     * Starts an instance whose step fails and is retried with a backoff.
     *
     * @param id the workflow id, which also decides the owning segment.
     */
    public record RetryArmed(String id) {

    }
}
