/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.examples.workflow.loadrunner;

import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

import static io.axoniq.framework.workflow.dsl.api.EventAssociationsUtils.equalsTo;
import static io.axoniq.framework.workflow.dsl.api.EventAssociationsUtils.payloadProperty;
import static io.axoniq.framework.workflow.runtime.association.Associations.associate;

/**
 * The workflow the load runner starts many instances of.
 * <p>
 * Each instance runs four steps:
 * <ol>
 *     <li>{@code prepare}, an execute step that records when the instance was prepared;</li>
 *     <li>{@code coolDown}, a short sleep, so every instance passes through a timer;</li>
 *     <li>{@code awaitApproval}, which waits for a {@link LoadApproved} event of this instance, with a long
 *     timeout, so that most instances are still waiting when the application restarts;</li>
 *     <li>{@code finish}, an execute step that records whether the instance was approved or timed out.</li>
 * </ol>
 * A {@link LoadStarted} event starts an instance; its {@code loadId} is the workflow id.
 *
 * @author Allard Buijze
 */
public class LoadWorkflow {

    /**
     * The name of the workflow definition.
     */
    public static final String NAME = "LoadWorkflow";
    /**
     * The name of the step that waits for the approval.
     */
    public static final String AWAIT_APPROVAL_STEP = "awaitApproval";

    private static final Duration COOL_DOWN = Duration.ofSeconds(2);

    private final Duration approvalWait;

    /**
     * Creates the workflow, letting each instance wait the given duration for its approval.
     *
     * @param approvalWait how long an instance waits for its {@link LoadApproved} event before it times out
     */
    public LoadWorkflow(Duration approvalWait) {
        this.approvalWait = approvalWait;
    }

    /**
     * The body of the workflow.
     *
     * @param workflow the context of the running instance
     */
    public void execute(SimpleWorkflowContext workflow) {
        String loadId = workflow.workflowId();

        workflow.awaitExecute("prepare", Map.of(),
                              (processingContext, payload) -> Map.of("preparedAt", Instant.now().toString()));

        workflow.awaitSleep("coolDown", COOL_DOWN);

        var approval = workflow.waitForEvent(AWAIT_APPROVAL_STEP,
                                             LoadApproved.class,
                                             associate(payloadProperty("loadId"), equalsTo(loadId)),
                                             step -> step.timeout(approvalWait));
        approval.await();

        String outcome = approval.success() ? "approved" : "timedOut";
        workflow.awaitExecute("finish", Map.of("outcome", outcome), (processingContext, payload) -> payload);
    }

    /**
     * Starts an instance of the {@link LoadWorkflow}.
     *
     * @param loadId   the id of the instance, used as its workflow id
     * @param complete whether the load runner approves this instance, or lets it wait
     */
    public record LoadStarted(String loadId, boolean complete) {

    }

    /**
     * Approves the instance of the {@link LoadWorkflow} with the given id.
     *
     * @param loadId the id of the approved instance
     */
    public record LoadApproved(String loadId) {

    }
}
