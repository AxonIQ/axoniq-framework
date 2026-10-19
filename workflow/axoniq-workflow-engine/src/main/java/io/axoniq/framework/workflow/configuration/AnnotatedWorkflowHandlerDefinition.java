/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.configuration;

import io.axoniq.framework.workflow.annotation.Workflow;
import io.axoniq.framework.workflow.dsl.api.WorkflowContext;
import org.axonframework.common.annotation.AnnotationUtils;
import org.axonframework.common.annotation.Internal;
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
 * A {@code @Workflow} body is assigned {@link WorkflowTriggerMessage} as its message type and {@link WorkflowContext}
 * as its expected payload type, since the synthesized command that invokes it always carries the workflow context as
 * its payload.
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
                                      WorkflowTriggerMessage.class,
                                      WorkflowContext.class,
                                      parameterResolverFactory,
                                      messageStreamResolver
                              ));
    }
}
