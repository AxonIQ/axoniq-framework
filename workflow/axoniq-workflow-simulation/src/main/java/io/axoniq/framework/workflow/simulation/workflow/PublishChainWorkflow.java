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
package io.axoniq.framework.workflow.simulation.workflow;

import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.framework.workflow.runtime.api.execution.state.StepFailedException;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.PublishReplyEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.PublishRequestEvent;

import java.time.Duration;
import java.util.Map;

import static io.axoniq.framework.workflow.dsl.api.EventAssociationsUtils.equalsTo;
import static io.axoniq.framework.workflow.runtime.association.Associations.associate;
import static io.axoniq.framework.workflow.runtime.association.PayloadPropertyValueRetriever.payloadProperty;

/**
 * The publish-primitive workload (ADR-019): a request/reply chain between workflows carried entirely by
 * {@code ctx.awaitPublish}, plus two bystanders that pin the routing consequences of a published event.
 * <ul>
 *   <li><strong>Requester</strong> ({@code pubreq-}): registers its {@code waitForEvent} on the reply <em>first</em>,
 *   then publishes the request, then awaits the reply. The wait is registered before the event that answers it can
 *   exist, so the wake is owed deterministically (no registration race).</li>
 *   <li><strong>Responder</strong> ({@code pubresp-}): <em>started by</em> the published request (workflow-to-workflow
 *   start), handles it, publishes the reply.</li>
 *   <li><strong>Observer</strong> ({@code pubobs-}): also started by the published request, so one published event
 *   starts two workflows (1:N fan-out). It never publishes.</li>
 *   <li><strong>Waiter</strong> ({@code pwait-}, scenario-only): started by its own event, waits for the published
 *   request of a given order id. Registered before the publish it is woken; registered after the event passed, it is
 *   not (wait conditions are evaluated at live delivery only, the engine's documented semantics). Several running
 *   waiters on the same order id are all woken by the one published event (running-to-running, 1:N).</li>
 * </ul>
 * Every published event is the publisher's COMPLETED step: the request is the requester's {@link #STEP_PUBLISH_REQUEST},
 * the reply is the responder's {@link #STEP_PUBLISH_REPLY}. The step names are deliberately unique across the four
 * bodies so a step of one instance showing up in another instance's state is unambiguous (INV-29).
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class PublishChainWorkflow {

    /**
     * Logical workflow name of the requester.
     */
    public static final String REQUESTER_WORKFLOW_NAME = "PublishRequesterWorkflow";
    /**
     * Logical workflow name of the responder.
     */
    public static final String RESPONDER_WORKFLOW_NAME = "PublishResponderWorkflow";
    /**
     * Logical workflow name of the observer.
     */
    public static final String OBSERVER_WORKFLOW_NAME = "PublishObserverWorkflow";
    /**
     * Logical workflow name of the waiter.
     */
    public static final String WAITER_WORKFLOW_NAME = "PublishWaiterWorkflow";

    /**
     * Id prefix of requester instances.
     */
    public static final String REQUESTER_ID_PREFIX = "pubreq-";
    /**
     * Id prefix of responder instances.
     */
    public static final String RESPONDER_ID_PREFIX = "pubresp-";
    /**
     * Id prefix of observer instances.
     */
    public static final String OBSERVER_ID_PREFIX = "pubobs-";
    /**
     * Id prefix of waiter instances.
     */
    public static final String WAITER_ID_PREFIX = "pwait-";

    /**
     * Requester: counting execute step before the publish.
     */
    public static final String STEP_PREPARE = "prepare";
    /**
     * Requester: the wait on the reply, registered before the request is published.
     */
    public static final String STEP_AWAIT_REPLY = "awaitReply";
    /**
     * Requester: the publish step whose event is the {@link PublishRequestEvent}.
     */
    public static final String STEP_PUBLISH_REQUEST = "publishRequest";
    /**
     * Requester: counting execute step after the reply arrived.
     */
    public static final String STEP_FINALIZE = "finalize";
    /**
     * Responder: counting execute step before the reply is published.
     */
    public static final String STEP_HANDLE_REQUEST = "handleRequest";
    /**
     * Responder: the publish step whose event is the {@link PublishReplyEvent}.
     */
    public static final String STEP_PUBLISH_REPLY = "publishReply";
    /**
     * Observer: its single counting execute step.
     */
    public static final String STEP_OBSERVE = "observe";
    /**
     * Waiter: the wait on the published request.
     */
    public static final String STEP_AWAIT_REQUEST = "awaitRequest";
    /**
     * Waiter: counting execute step after the request arrived.
     */
    public static final String STEP_AFTER_REQUEST = "afterRequest";

    /**
     * Effectively infinite wait timeout, so a chaos clock skew of hours never turns a wait into a TIMED_OUT (mirrors
     * {@link OrderWorkflow}).
     */
    public static final Duration WAIT_TIMEOUT = Duration.ofDays(365);

    private final CountingEffects effects;

    /**
     * Creates the workflow bodies recording into the given effects registry.
     *
     * @param effects counting side-effect registry.
     */
    public PublishChainWorkflow(CountingEffects effects) {
        this.effects = effects;
    }

    /**
     * Requester body: wait registered first, then publish, then await the reply.
     *
     * @param ctx workflow context.
     */
    public void executeRequester(SimpleWorkflowContext ctx) {
        try {
            String workflowId = ctx.workflowId();
            String orderId = String.valueOf(ctx.workflowPayload().get("orderId"));
            ctx.awaitExecute(STEP_PREPARE, Map.of(),
                             (pc, payload) -> Map.of("prepared", effects.record(workflowId, STEP_PREPARE)));
            var reply = ctx.waitForEvent(STEP_AWAIT_REPLY, PublishReplyEvent.class,
                                         associate(payloadProperty("orderId"), equalsTo(orderId)),
                                         step -> step.timeout(WAIT_TIMEOUT));
            ctx.awaitPublish(STEP_PUBLISH_REQUEST, new PublishRequestEvent(orderId));
            reply.await();
            ctx.awaitExecute(STEP_FINALIZE, Map.of(),
                             (pc, payload) -> Map.of("finalized", effects.record(workflowId, STEP_FINALIZE)));
        } catch (StepFailedException e) {
            // The documented recipe (INV-16): an uncaught step failure is not auto-failed by the engine. A step
            // in flight when the worker crashed comes back FAILED with StepIndeterminateException (at-most-once,
            // the F-0 fix); the body decides, and here it terminates the instance explicitly.
            ctx.fail(e);
        }
    }

    /**
     * Responder body: started by the published request, publishes the reply.
     *
     * @param ctx workflow context.
     */
    public void executeResponder(SimpleWorkflowContext ctx) {
        try {
            String workflowId = ctx.workflowId();
            String orderId = String.valueOf(ctx.workflowPayload().get("orderId"));
            ctx.awaitExecute(STEP_HANDLE_REQUEST, Map.of(),
                             (pc, payload) -> Map.of("handled", effects.record(workflowId, STEP_HANDLE_REQUEST)));
            ctx.awaitPublish(STEP_PUBLISH_REPLY, new PublishReplyEvent(orderId));
        } catch (StepFailedException e) {
            // The documented recipe (INV-16): an uncaught step failure is not auto-failed by the engine. A step
            // in flight when the worker crashed comes back FAILED with StepIndeterminateException (at-most-once,
            // the F-0 fix); the body decides, and here it terminates the instance explicitly.
            ctx.fail(e);
        }
    }

    /**
     * Observer body: started by the published request, records one effect and completes.
     *
     * @param ctx workflow context.
     */
    public void executeObserver(SimpleWorkflowContext ctx) {
        try {
            String workflowId = ctx.workflowId();
            ctx.awaitExecute(STEP_OBSERVE, Map.of(),
                             (pc, payload) -> Map.of("observed", effects.record(workflowId, STEP_OBSERVE)));
        } catch (StepFailedException e) {
            // The documented recipe (INV-16): an uncaught step failure is not auto-failed by the engine. A step
            // in flight when the worker crashed comes back FAILED with StepIndeterminateException (at-most-once,
            // the F-0 fix); the body decides, and here it terminates the instance explicitly.
            ctx.fail(e);
        }
    }

    /**
     * Waiter body: waits for the published request whose {@code orderId} equals this waiter's {@code awaitedOrderId}.
     *
     * @param ctx workflow context.
     */
    public void executeWaiter(SimpleWorkflowContext ctx) {
        try {
            String workflowId = ctx.workflowId();
            String awaitedOrderId = String.valueOf(ctx.workflowPayload().get("awaitedOrderId"));
            ctx.awaitEvent(STEP_AWAIT_REQUEST, PublishRequestEvent.class,
                           associate(payloadProperty("orderId"), equalsTo(awaitedOrderId)),
                           step -> step.timeout(WAIT_TIMEOUT));
            ctx.awaitExecute(STEP_AFTER_REQUEST, Map.of(),
                             (pc, payload) -> Map.of("after", effects.record(workflowId, STEP_AFTER_REQUEST)));
        } catch (StepFailedException e) {
            // The documented recipe (INV-16): an uncaught step failure is not auto-failed by the engine. A step
            // in flight when the worker crashed comes back FAILED with StepIndeterminateException (at-most-once,
            // the F-0 fix); the body decides, and here it terminates the instance explicitly.
            ctx.fail(e);
        }
    }
}
