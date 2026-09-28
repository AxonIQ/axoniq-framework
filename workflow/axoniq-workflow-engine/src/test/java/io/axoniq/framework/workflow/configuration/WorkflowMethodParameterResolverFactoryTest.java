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
import io.axoniq.framework.workflow.annotation.WorkflowStatusChangedHandler;
import io.axoniq.framework.workflow.dsl.api.WorkflowContext;
import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import org.axonframework.messaging.core.annotation.ParameterResolver;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.junit.jupiter.api.*;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static io.axoniq.framework.workflow.configuration.WorkflowMethodParameterResolverFactory.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

/**
 * Test class validating the {@link WorkflowMethodParameterResolverFactory}.
 *
 * @author Steven van Beelen
 */
class WorkflowMethodParameterResolverFactoryTest {

    private final WorkflowMethodParameterResolverFactory testSubject = new WorkflowMethodParameterResolverFactory();

    @Test
    void resolvesWorkflowContextFromItsResourceKey() {
        TestWorkflowContext context = mock(TestWorkflowContext.class);
        ProcessingContext processingContext =
                new StubProcessingContext().withResource(WORKFLOW_CONTEXT_RESOURCE_KEY, context);

        ParameterResolver<?> resolver = testSubject.createInstance(methodNamed("body"), parametersOf("body"), 0);

        assertThat(resolver).isNotNull();
        assertThat(
                resolver.resolveParameterValue(processingContext)
                        .orTimeout(50, TimeUnit.MILLISECONDS)
                        .join()
        ).isSameAs(context);
    }

    @Test
    void resolvesWorkflowStatusFromItsResourceKey() {
        ProcessingContext processingContext = new StubProcessingContext()
                .withResource(WORKFLOW_STATUS_RESOURCE_KEY, WorkflowStatus.COMPLETED);

        ParameterResolver<?> resolver =
                testSubject.createInstance(methodNamed("onStatusChanged"), parametersOf("onStatusChanged"), 0);

        assertThat(resolver).isNotNull();
        assertThat(
                resolver.resolveParameterValue(processingContext)
                        .orTimeout(50, TimeUnit.MILLISECONDS)
                        .join()
        ).isEqualTo(WorkflowStatus.COMPLETED);
    }

    @Test
    void resolvesAWrapperTypeByWrappingTheWorkflowContextResource() {
        TestWorkflowContext context = mock(TestWorkflowContext.class);
        ProcessingContext processingContext =
                new StubProcessingContext().withResource(WORKFLOW_CONTEXT_RESOURCE_KEY, context);

        ParameterResolver<?> resolver = testSubject.createInstance(methodNamed("wrapped"), parametersOf("wrapped"), 0);

        assertThat(resolver).isNotNull();
        Object resolved = resolver.resolveParameterValue(processingContext)
                                  .orTimeout(50, TimeUnit.MILLISECONDS)
                                  .join();
        assertThat(resolved).isInstanceOf(ContextWrapper.class);
        assertThat(((ContextWrapper) resolved).context()).isSameAs(context);
    }

    @Test
    void returnsNullForAnUnrelatedParameterType() {
        assertThat(testSubject.createInstance(methodNamed("unrelated"), parametersOf("unrelated"), 0)).isNull();
    }

    @Test
    void returnsNullForAnObjectTypedParameterOnANonWorkflowMethod() {
        // Without the isWorkflowAnnotated(...) gate, an Object-typed parameter would be trivially assignable from
        // any declaring class and get hijacked by the declaring-instance resolver, even on unrelated methods.
        assertThat(testSubject.createInstance(
                methodNamed("unrelatedWithObjectParameter"), parametersOf("unrelatedWithObjectParameter"), 0
        )).isNull();
    }

    private static java.lang.reflect.Parameter[] parametersOf(String name) {
        return methodNamed(name).getParameters();
    }

    private static Method methodNamed(String name) {
        return Arrays.stream(SampleWorkflow.class.getDeclaredMethods())
                     .filter(m -> m.getName().equals(name))
                     .findFirst()
                     .orElseThrow();
    }

    @SuppressWarnings("unused")
    static class SampleWorkflow {

        @Workflow
        void body(TestWorkflowContext context) {
        }

        @WorkflowStatusChangedHandler(workflowStatus = WorkflowStatus.COMPLETED)
        void onStatusChanged(WorkflowStatus status) {
        }

        @Workflow
        void wrapped(ContextWrapper wrapper) {
        }

        void unrelated(String notAWorkflowType) {
        }

        void unrelatedWithObjectParameter(Object notAWorkflowParameter) {
        }
    }

    interface TestWorkflowContext extends WorkflowContext {

    }

    public record ContextWrapper(WorkflowContext context) {

    }
}
