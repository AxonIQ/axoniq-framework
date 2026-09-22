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

import io.axoniq.framework.workflow.runtime.api.annotation.Workflow;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowContext;
import org.axonframework.common.annotation.AnnotationUtils;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.annotation.HandlerDefinition;
import org.axonframework.messaging.core.annotation.MessageHandlingMember;
import org.axonframework.messaging.core.annotation.MethodInvokingMessageHandlingMember;
import org.axonframework.messaging.core.annotation.ParameterResolverFactory;

import java.lang.reflect.Method;
import java.util.Optional;
import java.util.function.Function;

/**
 * {@link HandlerDefinition} recognizing {@link Workflow}-annotated methods, building a
 * {@link MethodInvokingMessageHandlingMember} for each.
 * <p>
 * A {@code @Workflow} body is assigned {@link WorkflowCommandMessage} as its message type and {@link WorkflowContext}
 * as its expected payload type, since the synthesized command that invokes it always carries the workflow context as
 * its payload. The body responds to an internally synthesized request to resume/run the workflow, never to the event
 * that originally triggered it.
 *
 * @author Steven van Beelen
 * @since 5.4.0
 */
@Internal
public class AnnotatedWorkflowHandlerDefinition implements HandlerDefinition {

    @Override
    public <T> Optional<MessageHandlingMember<T>> createHandler(
            Class<T> declaringType,
            Method method,
            ParameterResolverFactory parameterResolverFactory,
            Function<Object, MessageStream<?>> messageStreamResolver
    ) {
        return AnnotationUtils.findAnnotationAttributes(method, Workflow.class)
                              .map(attr -> new MethodInvokingMessageHandlingMember<>(
                                      method,
                                      WorkflowCommandMessage.class,
                                      WorkflowContext.class,
                                      parameterResolverFactory,
                                      messageStreamResolver
                              ));
    }
}
