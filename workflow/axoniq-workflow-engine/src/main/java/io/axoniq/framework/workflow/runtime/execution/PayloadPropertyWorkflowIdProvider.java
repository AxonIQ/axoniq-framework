/*
 * Copyright (c) 2010-2026. Axoniq B.V.
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
 * Workflow id provider reading the id from a property of the event's payload, optionally transforming it with an id
 * processor.
 * <p>
 * The derived id must be a valid workflow id, see {@link WorkflowIdProvider}: at least one and at most 255
 * characters, all printable ASCII. When the property may hold a longer or non-ASCII value, use the id processor to
 * derive a valid id from it, for example its SHA-256 hash:
 * <pre>{@code
 * new PayloadPropertyWorkflowIdProvider("documentPath", path -> path == null ? null : sha256Hex(path))
 * }</pre>
 * The id processor also receives {@code null} when the payload lacks the property, and should then return {@code null},
 * so the engine reports that no id could be derived.
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
     * @param idProcessor   processor called on the result of attribute extractor, or on {@code null} when the
     *                      payload lacks the attribute; must return a valid workflow id, see
     *                      {@link WorkflowIdProvider}
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
