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
import io.axoniq.framework.workflow.dsl.api.WorkflowContext;
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
 * Test class validating the {@link AnnotatedWorkflowHandlerDefinition}.
 *
 * @author Steven van Beelen
 */
class AnnotatedWorkflowHandlerDefinitionTest {

    private final AnnotatedWorkflowHandlerDefinition testSubject = new AnnotatedWorkflowHandlerDefinition();

    private final WorkflowMethodParameterResolverFactory parameterResolverFactory =
            new WorkflowMethodParameterResolverFactory();

    @Test
    void recognizesAWorkflowAnnotatedMethodAsAWorkflowTriggerMessageHandler() {
        // given / when...
        Optional<MessageHandlingMember<SampleWorkflow>> optionalMember =
                testSubject.createHandler(SampleWorkflow.class,
                                          methodNamed("body"),
                                          parameterResolverFactory,
                                          result -> MessageStream.empty());
        // then...
        assertThat(optionalMember).isPresent();
        assertThat(optionalMember.get().canHandleMessageType(WorkflowTriggerMessage.class)).isTrue();
        assertThat(optionalMember.get().canHandleMessageType(CommandMessage.class)).isFalse();
        assertThat(optionalMember.get().canHandleMessageType(EventMessage.class)).isFalse();
    }

    @Test
    void ignoresMethodWithoutWorkflowAnnotation() {
        // given / when...
        Optional<MessageHandlingMember<SampleWorkflow>> optionalMemberHandler =
                testSubject.createHandler(SampleWorkflow.class,
                                          methodNamed("unrelated"),
                                          parameterResolverFactory,
                                          result -> MessageStream.empty());
        // then...
        assertThat(optionalMemberHandler).isEmpty();
    }

    private static Method methodNamed(String name) {
        return Arrays.stream(SampleWorkflow.class.getDeclaredMethods())
                     .filter(m -> m.getName().equals(name))
                     .findFirst()
                     .orElseThrow();
    }

    @SuppressWarnings("unused")
    static class SampleWorkflow {

        @Workflow(startOnEventName = "start", idProperty = "id")
        void body(WorkflowContext context) {
        }

        void unrelated(WorkflowContext context) {
        }
    }
}
