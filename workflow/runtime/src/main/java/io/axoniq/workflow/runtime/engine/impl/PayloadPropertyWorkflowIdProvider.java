/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://lp.axoniq.io/axoniq-software-subscription-agreement-terms
 *
 *
 */
package io.axoniq.workflow.runtime.engine.impl;

import io.axoniq.workflow.runtime.api.WorkflowIdProvider;
import jakarta.annotation.Nonnull;
import org.axonframework.common.TypeReference;
import org.axonframework.conversion.Converter;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

/**
 * Workflow id provider accessing event message property.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
public class PayloadPropertyWorkflowIdProvider implements WorkflowIdProvider {

    private final Converter converter;
    private final String attributeName;
    private final Function<String, String> idProcessor;

    /**
     * Constructs id provider.
     *
     * @param converter     converter to access event message payload.
     * @param attributeName name of the payload attribute.
     */
    public PayloadPropertyWorkflowIdProvider(
            @Nonnull Converter converter,
            @Nonnull String attributeName) {
        this(converter, attributeName, Function.identity());
    }

    /**
     * Constructs id provider.
     *
     * @param converter     converter to access event message payload.
     * @param attributeName name of the payload attribute.
     * @param idProcessor   processor called on the result of attribute extractor.
     */
    public PayloadPropertyWorkflowIdProvider(
            @Nonnull Converter converter,
            @Nonnull String attributeName,
            @Nonnull Function<String, String> idProcessor
    ) {
        this.converter = Objects.requireNonNull(converter, "Converter must not be null");
        this.attributeName = Objects.requireNonNull(attributeName, "Attribute name must not be null");
        this.idProcessor = idProcessor;
    }

    @Override
    public String apply(EventMessage eventMessage) {
        return idProcessor
                .apply(Optional.ofNullable(eventMessage.payloadAs(
                                       new TypeReference<Map<String, Object>>() {
                                       },
                                       converter
                               )).flatMap(p -> Optional.ofNullable(p.get(attributeName)))
                               .map(Object::toString)
                               .orElse(null));
    }
}
