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

package io.axoniq.workflow.configuration;

import io.axoniq.workflow.runtime.api.annotation.Workflow;
import io.axoniq.workflow.runtime.api.annotation.WorkflowCancelledHandler;
import io.axoniq.workflow.runtime.api.annotation.WorkflowCompletedHandler;
import io.axoniq.workflow.runtime.api.annotation.WorkflowFailedHandler;
import io.axoniq.workflow.runtime.api.annotation.WorkflowStartedHandler;
import io.axoniq.workflow.runtime.api.annotation.WorkflowStatusChangedHandler;
import io.axoniq.workflow.runtime.api.annotation.WorkflowTimedOutHandler;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.workflow.runtime.api.execution.status.WorkflowStatus;
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
 * @since 1.0.0
 */
class AutoDetectionLifecycleListenerTest {

    @Test
    void detectLifecycleListenersWithoutName() {
        var workflow = new WorkflowWithListeners();
        Map<WorkflowStatus, CompositeWorkflowStatusChangeListener> listeners =
                AutoDetectionUtils.statusChangeListeners(workflow,
                                                         TestWorkflowContext.class,
                                                         "workflowName");

        assertThat(listeners).containsKey(WorkflowStatus.COMPLETED);
        var successListeners = listeners.get(WorkflowStatus.COMPLETED);

        TestWorkflowContext context = mock(TestWorkflowContext.class);
        successListeners.onWorkflowStatus(WorkflowStatus.COMPLETED, context);

        assertThat(workflow.invoked).containsExactly("onSuccessNoName");
    }

    @Test
    void detectLifecycleListenersWithNameMatching() {
        var workflow = new WorkflowWithNamedListeners();
        Map<WorkflowStatus, CompositeWorkflowStatusChangeListener> listeners =
                AutoDetectionUtils.statusChangeListeners(workflow, TestWorkflowContext.class, "workflow-1");

        assertThat(listeners).containsKey(WorkflowStatus.COMPLETED);
        var successListeners = listeners.get(WorkflowStatus.COMPLETED);

        TestWorkflowContext context = mock(TestWorkflowContext.class);
        successListeners.onWorkflowStatus(WorkflowStatus.COMPLETED, context);

        assertThat(workflow.invoked).containsExactly("onSuccess1");
    }

    @Test
    void detectLifecycleListenersWithNameMismatch() {
        var workflow = new WorkflowWithNamedListeners();
        Map<WorkflowStatus, CompositeWorkflowStatusChangeListener> listeners =
                AutoDetectionUtils.statusChangeListeners(workflow, TestWorkflowContext.class, "other-workflow");

        assertThat(listeners).containsKey(WorkflowStatus.COMPLETED);
        var successListeners = listeners.get(WorkflowStatus.COMPLETED);

        TestWorkflowContext context = mock(TestWorkflowContext.class);
        successListeners.onWorkflowStatus(WorkflowStatus.COMPLETED, context);

        assertThat(workflow.invoked).isEmpty();
    }

    @Test
    void detectAllLifecycleListeners() {
        var workflow = new WorkflowWithAllListeners();
        Map<WorkflowStatus, CompositeWorkflowStatusChangeListener> listeners =
                AutoDetectionUtils.statusChangeListeners(workflow, TestWorkflowContext.class, "workflowName");

        TestWorkflowContext context = mock(TestWorkflowContext.class);

        listeners.get(WorkflowStatus.COMPLETED).onWorkflowStatus(WorkflowStatus.COMPLETED, context);
        listeners.get(WorkflowStatus.FAILED).onWorkflowStatus(WorkflowStatus.FAILED, context);
        listeners.get(WorkflowStatus.CANCELLED).onWorkflowStatus(WorkflowStatus.CANCELLED, context);
        listeners.get(WorkflowStatus.TIMED_OUT).onWorkflowStatus(WorkflowStatus.TIMED_OUT, context);

        assertThat(workflow.invoked).containsExactlyInAnyOrder("onSuccess", "onFailure", "onCancellation", "onTimeout");
    }

    @Test
    void detectStatusHandlerListener() {
        var workflow = new WorkflowWithStatusHandler();
        Map<WorkflowStatus, CompositeWorkflowStatusChangeListener> listeners =
                AutoDetectionUtils.statusChangeListeners(workflow, TestWorkflowContext.class, "workflowName");

        assertThat(listeners).containsKey(WorkflowStatus.STARTED);
        var successListeners = listeners.get(WorkflowStatus.STARTED);

        TestWorkflowContext context = mock(TestWorkflowContext.class);
        successListeners.onWorkflowStatus(WorkflowStatus.STARTED, context);

        assertThat(workflow.invoked).containsExactly("onStarted");
    }

    @Test
    void detectStartHandlerListener() {
        var workflow = new WorkflowWithStartHandler();
        Map<WorkflowStatus, CompositeWorkflowStatusChangeListener> listeners =
                AutoDetectionUtils.statusChangeListeners(workflow, TestWorkflowContext.class, "workflowName");

        assertThat(listeners).containsKey(WorkflowStatus.STARTED);
        var successListeners = listeners.get(WorkflowStatus.STARTED);

        TestWorkflowContext context = mock(TestWorkflowContext.class);
        successListeners.onWorkflowStatus(WorkflowStatus.STARTED, context);

        assertThat(workflow.invoked).containsExactly("onStart");
    }

    @Test
    void reproduceArgumentTypeMismatch() {
        var workflow = new WorkflowWithSpecializedContext();
        Map<WorkflowStatus, CompositeWorkflowStatusChangeListener> listeners =
                AutoDetectionUtils.statusChangeListeners(workflow, SpecializedWorkflowContext.class, "workflowName");

        assertThat(listeners).containsKey(WorkflowStatus.COMPLETED);
        var successListeners = listeners.get(WorkflowStatus.COMPLETED);

        SpecializedWorkflowContext context = mock(SpecializedWorkflowContext.class);
        // This should not throw IllegalArgumentException: argument type mismatch
        successListeners.onWorkflowStatus(WorkflowStatus.COMPLETED, context);

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
