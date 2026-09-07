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
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Map;

import static io.axoniq.framework.workflow.dsl.base.BaseWorkflowContext.equalsTo;
import static io.axoniq.framework.workflow.runtime.association.Associations.associate;
import static io.axoniq.framework.workflow.runtime.association.PayloadPropertyValueRetriever.payloadProperty;

/**
 * Two-phase workflow used by the sharding rig.
 * <p>
 * Both phases append a row to {@code rig_step_log} in the shared Postgres database, tagged with the node that ran
 * them. That table is the rig's oracle: it shows forward progress (a {@code resume} row committed after a node was
 * killed), which node made it, and duplication (more than one row for the same workflow and step).
 * <p>
 * The write sits inside a step body on purpose. A workflow body is re-executed from the top whenever the instance is
 * rehydrated, so anything outside a step would log once per restore and say nothing about duplicate work.
 */
@Component
public class RigWorkflow {

    private final JdbcTemplate jdbcTemplate;
    private final String nodeId;

    RigWorkflow(JdbcTemplate jdbcTemplate,
                @org.springframework.beans.factory.annotation.Value("${rig.node-id}") String nodeId) {
        this.jdbcTemplate = jdbcTemplate;
        this.nodeId = nodeId;
    }

    /**
     * Starts on {@link RigStartEvent}, records the start, waits for the correlated {@link RigResumeEvent} and records
     * the resume.
     *
     * @param ctx the workflow context.
     */
    @Workflow(
            idProperty = "id",
            startOnEventClass = RigStartEvent.class,
            workflowName = "RigWorkflow"
    )
    public void execute(SimpleWorkflowContext ctx) {
        ctx.awaitExecute("recordStart", Map.of(), (pc, payload) -> log(ctx.workflowId(), "start"));

        ctx.awaitEvent("waitForResume",
                       RigResumeEvent.class,
                       associate(payloadProperty("id"), equalsTo(ctx.workflowPayload().get("id"))),
                       step -> step.timeout(Duration.ofMinutes(10)));

        ctx.awaitExecute("recordResume", Map.of(), (pc, payload) -> log(ctx.workflowId(), "resume"));
    }

    private Map<String, Object> log(String workflowId, String step) {
        jdbcTemplate.update(
                "INSERT INTO rig_step_log (workflow_id, step, node_id, logged_at) VALUES (?, ?, ?, ?)",
                workflowId, step, nodeId, System.currentTimeMillis()
        );
        return Map.of(step + "By", nodeId);
    }

    /**
     * Starts a rig workflow instance.
     *
     * @param id the workflow id, which also decides the owning segment.
     */
    public record RigStartEvent(String id) {

    }

    /**
     * Resumes a rig workflow instance. It has no spawn candidate, so it is broadcast to every segment and only the
     * segment owning {@code id} wakes its instance.
     *
     * @param id the workflow id to resume.
     */
    public record RigResumeEvent(String id) {

    }
}
