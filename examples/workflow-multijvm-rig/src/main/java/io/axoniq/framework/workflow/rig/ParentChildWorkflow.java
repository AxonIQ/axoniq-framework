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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Map;

import static io.axoniq.framework.workflow.dsl.base.BaseWorkflowContext.equalsTo;
import static io.axoniq.framework.workflow.runtime.association.Associations.associate;
import static io.axoniq.framework.workflow.runtime.association.PayloadPropertyValueRetriever.payloadProperty;

/**
 * Two workflows that depend on each other while living on different segments.
 * <p>
 * The parent spawns the child by appending the child's start event, then waits for the child to report back. The
 * engine routes a start event by the id it carries, so a child id chosen to hash onto another segment starts its
 * instance in whichever process owns that segment - a hand-off between two nodes with no other coordination than the
 * event store.
 * <p>
 * Both halves record their steps in {@code rig_step_log}, tagged with the node that ran them, so the pair can be
 * followed while one of the two segments changes hands underneath it.
 */
@Component
public class ParentChildWorkflow {

    /**
     * Budget for the parent's wait on its child, and the child's wait on its release. Long enough that a segment
     * hand-off in between cannot be mistaken for a timeout.
     */
    public static final Duration WAIT_TIMEOUT = Duration.ofMinutes(10);

    private final JdbcTemplate jdbcTemplate;
    private final RigEventPublisher publisher;
    private final String nodeId;

    ParentChildWorkflow(JdbcTemplate jdbcTemplate, RigEventPublisher publisher,
                        @Value("${rig.node-id}") String nodeId) {
        this.jdbcTemplate = jdbcTemplate;
        this.publisher = publisher;
        this.nodeId = nodeId;
    }

    /**
     * Spawns the child named in the start event, then waits for it to complete.
     *
     * @param ctx the workflow context.
     */
    @Workflow(
            idProperty = "id",
            startOnEventClass = ParentStarted.class,
            workflowName = "ParentWorkflow"
    )
    public void parent(SimpleWorkflowContext ctx) {
        var childId = String.valueOf(ctx.workflowPayload().get("childId"));
        var child = ctx.waitForEvent("waitForChild",
                                     ChildCompleted.class,
                                     associate(payloadProperty("parentId"),
                                               equalsTo(ctx.workflowPayload().get("id"))),
                                     step -> step.timeout(WAIT_TIMEOUT));

        ctx.awaitExecute("spawnChild", Map.of(), (pc, payload) -> {
            publisher.publish(new ChildRequested(childId, ctx.workflowId()));
            return log(ctx.workflowId(), "parent-spawn");
        });

        child.await();
        ctx.awaitExecute("finishParent", Map.of(), (pc, payload) -> log(ctx.workflowId(), "parent-done"));
    }

    /**
     * Records that it is running, waits to be released, then reports back to its parent.
     *
     * @param ctx the workflow context.
     */
    @Workflow(
            idProperty = "id",
            startOnEventClass = ChildRequested.class,
            workflowName = "ChildWorkflow"
    )
    public void child(SimpleWorkflowContext ctx) {
        var parentId = String.valueOf(ctx.workflowPayload().get("parentId"));
        var release = ctx.waitForEvent("waitForRelease",
                                       ChildReleased.class,
                                       associate(payloadProperty("id"),
                                                 equalsTo(ctx.workflowPayload().get("id"))),
                                       step -> step.timeout(WAIT_TIMEOUT));

        ctx.awaitExecute("beginChild", Map.of(), (pc, payload) -> log(ctx.workflowId(), "child-start"));

        release.await();
        ctx.awaitExecute("finishChild", Map.of(), (pc, payload) -> {
            publisher.publish(new ChildCompleted(ctx.workflowId(), parentId));
            return log(ctx.workflowId(), "child-done");
        });
    }

    private Map<String, Object> log(String workflowId, String step) {
        jdbcTemplate.update(
                "INSERT INTO rig_step_log (workflow_id, step, node_id, logged_at) VALUES (?, ?, ?, ?)",
                workflowId, step, nodeId, System.currentTimeMillis()
        );
        return Map.of(step + "By", nodeId);
    }

    /**
     * Starts a parent instance.
     *
     * @param id      the parent's workflow id, which also decides the parent's segment.
     * @param childId the child to spawn, whose value decides the child's segment.
     */
    public record ParentStarted(String id, String childId) {

    }

    /**
     * Starts a child instance. Appended by the parent's own step, not by the rig.
     *
     * @param id       the child's workflow id, which also decides the child's segment.
     * @param parentId the parent to report back to.
     */
    public record ChildRequested(String id, String parentId) {

    }

    /**
     * Lets a waiting child finish. No spawn candidate, so it is broadcast to every segment.
     *
     * @param id the child to release.
     */
    public record ChildReleased(String id) {

    }

    /**
     * The child's report back to its parent. Appended by the child's own step.
     *
     * @param id       the child that finished.
     * @param parentId the parent whose wait it satisfies.
     */
    public record ChildCompleted(String id, String parentId) {

    }
}
