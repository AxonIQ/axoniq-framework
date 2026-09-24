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

package io.axoniq.framework.workflow.configuration;

import io.axoniq.framework.workflow.annotation.Workflow;
import io.axoniq.framework.workflow.annotation.WorkflowCancelledHandler;
import io.axoniq.framework.workflow.annotation.WorkflowCompletedHandler;
import io.axoniq.framework.workflow.annotation.WorkflowFailedHandler;
import io.axoniq.framework.workflow.annotation.WorkflowStartedHandler;
import io.axoniq.framework.workflow.annotation.WorkflowStatusChangedHandler;
import io.axoniq.framework.workflow.annotation.WorkflowTimedOutHandler;
import io.axoniq.framework.workflow.dsl.api.WorkflowContext;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowStatusChangeListener;
import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import io.axoniq.framework.workflow.runtime.util.DefaultTimeoutFutureResolver;
import io.axoniq.framework.workflow.runtime.util.FutureResolver;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.annotation.AnnotatedHandlerInspector;
import org.axonframework.messaging.core.annotation.AnnotatedMessageHandlingMemberDefinition;
import org.axonframework.messaging.core.annotation.HandlerEnhancerDefinition;
import org.axonframework.messaging.core.annotation.MessageHandlingMember;
import org.axonframework.messaging.core.annotation.MultiHandlerDefinition;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.junit.jupiter.api.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

/**
 * Test for autodetection of workflow lifecycle status listener registrations.
 *
 * @author Simon Zambrovski
 */
class AutoDetectionLifecycleListenerTest {

    private static AnnotatedHandlerInspector<Object> inspectorFor(Object instance) {
        MultiHandlerDefinition combinedHandlerDefinition = MultiHandlerDefinition.ordered(
                new HandlerEnhancerDefinition() {
                    @Override
                    public <T> MessageHandlingMember<T> wrapHandler(MessageHandlingMember<T> original) {
                        return original;
                    }
                },
                new AnnotatedWorkflowHandlerDefinition(),
                new AnnotatedWorkflowStatusChangedHandlerDefinition(),
                new AnnotatedMessageHandlingMemberDefinition()
        );
        //noinspection unchecked
        return AnnotatedHandlerInspector.inspectType(
                (Class<Object>) instance.getClass(),
                mock(MessageTypeResolver.class),
                new WorkflowMethodParameterResolverFactory(),
                combinedHandlerDefinition
        );
    }

    private static ProcessingContext processingContextWithFutureResolver() {
        return StubProcessingContext.withComponents(
                cr -> cr.registerComponent(FutureResolver.class, cfg -> new DefaultTimeoutFutureResolver())
        );
    }

    @Test
    void detectLifecycleListenersWithoutName() {
        WorkflowWithListeners workflow = new WorkflowWithListeners();
        Map<WorkflowStatus, WorkflowStatusChangeListener> listeners =
                AutoDetectionUtils.statusChangeListeners(
                        workflow, "workflowName", inspectorFor(workflow));

        assertThat(listeners).containsKey(WorkflowStatus.COMPLETED);
        WorkflowStatusChangeListener successListeners = listeners.get(WorkflowStatus.COMPLETED);

        TestWorkflowContext context = mock(TestWorkflowContext.class);
        EventMessage eventMessage = mock(EventMessage.class);
        successListeners.onWorkflowStatus(
                WorkflowStatus.COMPLETED, context, eventMessage, processingContextWithFutureResolver());

        assertThat(workflow.invoked).containsExactly("onSuccessNoName");
    }

    @Test
    void detectLifecycleListenersWithNameMatching() {
        WorkflowWithNamedListeners workflow = new WorkflowWithNamedListeners();
        Map<WorkflowStatus, WorkflowStatusChangeListener> listeners =
                AutoDetectionUtils.statusChangeListeners(
                        workflow, "workflow-1", inspectorFor(workflow));

        assertThat(listeners).containsKey(WorkflowStatus.COMPLETED);
        WorkflowStatusChangeListener successListeners = listeners.get(WorkflowStatus.COMPLETED);

        TestWorkflowContext context = mock(TestWorkflowContext.class);
        EventMessage eventMessage = mock(EventMessage.class);
        successListeners.onWorkflowStatus(
                WorkflowStatus.COMPLETED, context, eventMessage, processingContextWithFutureResolver());

        assertThat(workflow.invoked).containsExactly("onSuccess1");
    }

    @Test
    void detectLifecycleListenersWithNameMismatch() {
        WorkflowWithNamedListeners workflow = new WorkflowWithNamedListeners();
        Map<WorkflowStatus, WorkflowStatusChangeListener> listeners =
                AutoDetectionUtils.statusChangeListeners(
                        workflow, "other-workflow", inspectorFor(workflow));

        assertThat(listeners).containsKey(WorkflowStatus.COMPLETED);
        WorkflowStatusChangeListener successListeners = listeners.get(WorkflowStatus.COMPLETED);

        TestWorkflowContext context = mock(TestWorkflowContext.class);
        EventMessage eventMessage = mock(EventMessage.class);
        successListeners.onWorkflowStatus(
                WorkflowStatus.COMPLETED, context, eventMessage, processingContextWithFutureResolver());

        assertThat(workflow.invoked).isEmpty();
    }

    @Test
    void detectAllLifecycleListeners() {
        WorkflowWithAllListeners workflow = new WorkflowWithAllListeners();
        Map<WorkflowStatus, WorkflowStatusChangeListener> listeners =
                AutoDetectionUtils.statusChangeListeners(
                        workflow, "workflowName", inspectorFor(workflow));

        TestWorkflowContext context = mock(TestWorkflowContext.class);
        EventMessage eventMessage = mock(EventMessage.class);

        listeners.get(WorkflowStatus.COMPLETED)
                 .onWorkflowStatus(
                         WorkflowStatus.COMPLETED, context, eventMessage, processingContextWithFutureResolver());
        listeners.get(WorkflowStatus.FAILED)
                 .onWorkflowStatus(WorkflowStatus.FAILED, context, eventMessage, processingContextWithFutureResolver());
        listeners.get(WorkflowStatus.CANCELLED)
                 .onWorkflowStatus(
                         WorkflowStatus.CANCELLED, context, eventMessage, processingContextWithFutureResolver());
        listeners.get(WorkflowStatus.TIMED_OUT)
                 .onWorkflowStatus(
                         WorkflowStatus.TIMED_OUT, context, eventMessage, processingContextWithFutureResolver());

        assertThat(workflow.invoked).containsExactlyInAnyOrder("onSuccess", "onFailure", "onCancellation", "onTimeout");
    }

    @Test
    void detectStatusHandlerListener() {
        WorkflowWithStatusHandler workflow = new WorkflowWithStatusHandler();
        Map<WorkflowStatus, WorkflowStatusChangeListener> listeners =
                AutoDetectionUtils.statusChangeListeners(
                        workflow, "workflowName", inspectorFor(workflow));

        assertThat(listeners).containsKey(WorkflowStatus.STARTED);
        WorkflowStatusChangeListener successListeners = listeners.get(WorkflowStatus.STARTED);

        TestWorkflowContext context = mock(TestWorkflowContext.class);
        EventMessage eventMessage = mock(EventMessage.class);
        successListeners.onWorkflowStatus(
                WorkflowStatus.STARTED, context, eventMessage, processingContextWithFutureResolver());

        assertThat(workflow.invoked).containsExactly("onStarted");
    }

    @Test
    void detectStartHandlerListener() {
        WorkflowWithStartHandler workflow = new WorkflowWithStartHandler();
        Map<WorkflowStatus, WorkflowStatusChangeListener> listeners =
                AutoDetectionUtils.statusChangeListeners(
                        workflow, "workflowName", inspectorFor(workflow));

        assertThat(listeners).containsKey(WorkflowStatus.STARTED);
        WorkflowStatusChangeListener successListeners = listeners.get(WorkflowStatus.STARTED);

        TestWorkflowContext context = mock(TestWorkflowContext.class);
        EventMessage eventMessage = mock(EventMessage.class);
        successListeners.onWorkflowStatus(
                WorkflowStatus.STARTED, context, eventMessage, processingContextWithFutureResolver());

        assertThat(workflow.invoked).containsExactly("onStart");
    }

    @Test
    void reproduceArgumentTypeMismatch() {
        WorkflowWithSpecializedContext workflow = new WorkflowWithSpecializedContext();
        Map<WorkflowStatus, WorkflowStatusChangeListener> listeners =
                AutoDetectionUtils.statusChangeListeners(
                        workflow, "workflowName", inspectorFor(workflow));

        assertThat(listeners).containsKey(WorkflowStatus.COMPLETED);
        WorkflowStatusChangeListener successListeners = listeners.get(WorkflowStatus.COMPLETED);

        SpecializedWorkflowContext context = mock(SpecializedWorkflowContext.class);
        EventMessage eventMessage = mock(EventMessage.class);
        // This should not throw IllegalArgumentException: argument type mismatch
        successListeners.onWorkflowStatus(
                WorkflowStatus.COMPLETED, context, eventMessage, processingContextWithFutureResolver());

        assertThat(workflow.invoked).containsExactly("onSuccessSpecialized");
    }

    public interface TestWorkflowContext extends WorkflowContext {

    }

    interface SpecializedWorkflowContext extends WorkflowContext {

        void specializedMethod();
    }

    public static class WorkflowWithListeners {

        public List<String> invoked = new ArrayList<>();

        @Workflow(startOnEventName = "start", idProperty = "id")
        public void myWorkflow(TestWorkflowContext context) {
        }

        @WorkflowCompletedHandler
        public void onSuccessNoName(WorkflowStatus status, TestWorkflowContext context) {
            invoked.add("onSuccessNoName");
        }
    }

    public static class WorkflowWithNamedListeners {

        public List<String> invoked = new ArrayList<>();

        @Workflow(workflowName = "workflow-1", startOnEventName = "start", idProperty = "id")
        public void myWorkflow1(TestWorkflowContext context) {
        }

        @Workflow(workflowName = "workflow-2", startOnEventName = "start", idProperty = "id")
        public void myWorkflow2(TestWorkflowContext context) {
        }

        @WorkflowCompletedHandler(workflowName = "workflow-1")
        public void onSuccess1(WorkflowStatus status, TestWorkflowContext context) {
            invoked.add("onSuccess1");
        }

        @WorkflowCompletedHandler(workflowName = "workflow-2")
        public void onSuccess2(WorkflowStatus status, TestWorkflowContext context) {
            invoked.add("onSuccess2");
        }
    }

    public static class WorkflowWithAllListeners {

        public List<String> invoked = new ArrayList<>();

        @WorkflowCompletedHandler
        public void onSuccess(WorkflowStatus status, TestWorkflowContext context) {
            invoked.add("onSuccess");
        }

        @WorkflowFailedHandler
        public void onFailure(WorkflowStatus status, TestWorkflowContext context) {
            invoked.add("onFailure");
        }

        @WorkflowCancelledHandler
        public void onCancellation(WorkflowStatus status, TestWorkflowContext context) {
            invoked.add("onCancellation");
        }

        @WorkflowTimedOutHandler
        public void onTimeout(WorkflowStatus status, TestWorkflowContext context) {
            invoked.add("onTimeout");
        }
    }

    public static class WorkflowWithSpecializedContext {

        public List<String> invoked = new ArrayList<>();

        @WorkflowCompletedHandler
        public void onSuccessSpecialized(WorkflowStatus status, SpecializedWorkflowContext context) {
            invoked.add("onSuccessSpecialized");
        }
    }

    public static class WorkflowWithStatusHandler {

        public List<String> invoked = new ArrayList<>();

        @WorkflowStatusChangedHandler(workflowStatus = WorkflowStatus.STARTED)
        public void onStarted(WorkflowStatus status, TestWorkflowContext context) {
            invoked.add("onStarted");
        }
    }

    public static class WorkflowWithStartHandler {

        public List<String> invoked = new ArrayList<>();

        @WorkflowStartedHandler
        public void onStart(WorkflowStatus status, TestWorkflowContext context) {
            invoked.add("onStart");
        }
    }
}
