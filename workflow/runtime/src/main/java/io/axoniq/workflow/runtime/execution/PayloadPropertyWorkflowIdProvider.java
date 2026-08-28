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
package io.axoniq.workflow.runtime.execution;

import io.axoniq.workflow.runtime.api.execution.context.WorkflowIdProvider;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import static io.axoniq.workflow.runtime.execution.EventSourcedWorkflowState.PAYLOAD_TYPE;

/**
 * Workflow id provider accessing event message property.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
public class PayloadPropertyWorkflowIdProvider implements WorkflowIdProvider {

    private final String attributeName;
    private final Function<String, String> idProcessor;

    public static PayloadPropertyWorkflowIdProvider fromPayloadAttribute(
            Configuration configuration,
            String attributeName,
            UnaryOperator<String> customizer) {
        return new PayloadPropertyWorkflowIdProvider(
                attributeName,
                customizer
        );
    }

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

    @Override
    public String apply(EventMessage eventMessage) {
        return idProcessor
                .apply(Optional.ofNullable(eventMessage.payloadAs(PAYLOAD_TYPE))
                               .flatMap(p -> Optional.ofNullable(p.get(attributeName)))
                               .map(Object::toString)
                               .orElse(null));
    }
}
