/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowIdProvider;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import static io.axoniq.framework.workflow.runtime.execution.EventSourcedWorkflowState.PAYLOAD_TYPE;

/**
 * Workflow id provider accessing event message property.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
public class PayloadPropertyWorkflowIdProvider implements WorkflowIdProvider {

    private final String attributeName;
    private final Function<String, String> idProcessor;

    /**
     * Constructs id provider.
     *
     * @param attributeName name of the payload attribute.
     */
    public PayloadPropertyWorkflowIdProvider(
            String attributeName) {
        this(attributeName, UnaryOperator.identity());
    }

    /**
     * Constructs id provider.
     *
     * @param attributeName name of the payload attribute.
     * @param idProcessor   processor called on the result of attribute extractor.
     */
    public PayloadPropertyWorkflowIdProvider(
            String attributeName,
            UnaryOperator<String> idProcessor
    ) {
        this.attributeName = Objects.requireNonNull(attributeName, "Attribute name must not be null");
        this.idProcessor = idProcessor;
    }

    public static PayloadPropertyWorkflowIdProvider fromPayloadAttribute(
            Configuration configuration,
            String attributeName,
            UnaryOperator<String> customizer) {
        return new PayloadPropertyWorkflowIdProvider(
                attributeName,
                customizer
        );
    }

    @Override
    public String apply(EventMessage eventMessage) {
        return idProcessor
                .apply(Optional.ofNullable(eventMessage.payloadAs(PAYLOAD_TYPE))
                               .flatMap(p -> Optional.ofNullable(p.get(attributeName)))
                               .map(Object::toString)
                               .orElse(null));
    }
}
