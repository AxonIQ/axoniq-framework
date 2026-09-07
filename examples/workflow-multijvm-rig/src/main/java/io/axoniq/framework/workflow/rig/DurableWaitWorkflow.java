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
import io.axoniq.framework.workflow.runtime.api.execution.state.WorkflowStepResult;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Map;

import static io.axoniq.framework.workflow.dsl.base.BaseWorkflowContext.equalsTo;
import static io.axoniq.framework.workflow.runtime.association.Associations.associate;
import static io.axoniq.framework.workflow.runtime.association.PayloadPropertyValueRetriever.payloadProperty;

/**
 * Subscribe-first workflow: the wait is registered before the work that triggers the awaited event is done.
 * <p>
 * {@link RigWorkflow} blocks its driver thread inside the wait. This one does not: it starts the wait asynchronously,
 * runs a step while it is outstanding, and only then joins on it. The instance therefore sits with a durably recorded,
 * unsatisfied wait while its driver thread is busy elsewhere, which is the state a segment rebalance has to preserve.
 * <p>
 * One row lands in {@code rig_step_log} per instance, {@code request}, once the payment has been requested. There is
 * deliberately no step after the wait: a step in flight when its node is killed is never re-run under the at-most-once
 * guarantee, so its absence would say nothing about whether the wait was ever satisfied. Whether the wait completed is
 * read out of the event store instead.
 */
@Component
public class DurableWaitWorkflow {

    private final JdbcTemplate jdbcTemplate;
    private final String nodeId;

    DurableWaitWorkflow(JdbcTemplate jdbcTemplate, @Value("${rig.node-id}") String nodeId) {
        this.jdbcTemplate = jdbcTemplate;
        this.nodeId = nodeId;
    }

    /**
     * Subscribes to {@link PaymentReceived}, requests the payment, then joins on the wait.
     *
     * @param ctx the workflow context.
     */
    @Workflow(
            idProperty = "id",
            startOnEventClass = OrderPlaced.class,
            workflowName = "DurableWaitWorkflow"
    )
    public void execute(SimpleWorkflowContext ctx) {
        var payment = ctx.waitForEvent("waitForPayment",
                                       PaymentReceived.class,
                                       associate(payloadProperty("id"), equalsTo(ctx.workflowPayload().get("id"))),
                                       step -> step.timeout(Duration.ofMinutes(9)));

        ctx.awaitExecute("requestPayment", Map.of(), (pc, payload) -> log(ctx.workflowId(), "request"));

        ctx.allMatch(WorkflowStepResult::isCompleted, payment).await();
    }

    private Map<String, Object> log(String workflowId, String step) {
        jdbcTemplate.update(
                "INSERT INTO rig_step_log (workflow_id, step, node_id, logged_at) VALUES (?, ?, ?, ?)",
                workflowId, step, nodeId, System.currentTimeMillis()
        );
        return Map.of(step + "By", nodeId);
    }

    /**
     * Starts a durable-wait instance.
     *
     * @param id the workflow id, which also decides the owning segment.
     */
    public record OrderPlaced(String id) {

    }

    /**
     * Satisfies the instance's outstanding wait. No spawn candidate, so it is broadcast to every segment.
     *
     * @param id the workflow id whose wait it satisfies.
     */
    public record PaymentReceived(String id) {

    }
}
