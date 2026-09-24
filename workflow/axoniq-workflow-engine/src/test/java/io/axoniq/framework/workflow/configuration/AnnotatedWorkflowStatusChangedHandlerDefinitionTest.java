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

import io.axoniq.framework.workflow.annotation.WorkflowCompletedHandler;
import io.axoniq.framework.workflow.dsl.api.WorkflowContext;
import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.annotation.MessageHandlingMember;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.junit.jupiter.api.*;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test class validating the {@link AnnotatedWorkflowStatusChangedHandlerDefinition}.
 *
 * @author Steven van Beelen
 */
class AnnotatedWorkflowStatusChangedHandlerDefinitionTest {

    private final WorkflowMethodParameterResolverFactory parameterResolverFactory =
            new WorkflowMethodParameterResolverFactory();

    private final AnnotatedWorkflowStatusChangedHandlerDefinition testSubject =
            new AnnotatedWorkflowStatusChangedHandlerDefinition();

    @Test
    void recognizesAWorkflowStatusChangedHandlerAnnotatedMethodAsAnEventMessageHandler() {
        // given/when...
        Optional<MessageHandlingMember<SampleWorkflow>> optionalHandler =
                testSubject.createHandler(SampleWorkflow.class,
                                          methodNamed("onCompleted"),
                                          parameterResolverFactory,
                                          result -> MessageStream.empty());
        // then...
        assertThat(optionalHandler).isPresent();
        assertThat(optionalHandler.get().canHandleMessageType(EventMessage.class)).isTrue();
        assertThat(optionalHandler.get().canHandleMessageType(CommandMessage.class)).isFalse();
    }

    @Test
    void ignoresAMethodCarryingNeitherAnnotation() {
        // given/when...
        Optional<MessageHandlingMember<SampleWorkflow>> optionalHandler =
                testSubject.createHandler(SampleWorkflow.class,
                                          methodNamed("unrelated"),
                                          parameterResolverFactory,
                                          result -> MessageStream.empty());
        // then...
        assertThat(optionalHandler).isEmpty();
    }

    private static Method methodNamed(String name) {
        return Arrays.stream(SampleWorkflow.class.getDeclaredMethods())
                     .filter(m -> m.getName().equals(name))
                     .findFirst()
                     .orElseThrow();
    }

    @SuppressWarnings("unused")
    static class SampleWorkflow {

        @WorkflowCompletedHandler
        void onCompleted(WorkflowStatus status, WorkflowContext context) {
        }

        void unrelated(WorkflowContext context) {
        }
    }
}
