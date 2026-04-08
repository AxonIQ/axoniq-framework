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
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */

package org.axonframework.modelling.command.inspection;

import org.axonframework.messaging.core.annotation.HandlerAttributes;
import org.axonframework.messaging.core.annotation.HandlerEnhancerDefinition;
import org.axonframework.messaging.core.annotation.MessageHandlingMember;
import org.axonframework.messaging.core.annotation.WrappedMessageHandlingMember;
import org.axonframework.modelling.command.AggregateCreationPolicy;
import org.axonframework.modelling.command.CreationPolicy;


/**
 * Implementation of {@link HandlerEnhancerDefinition} used for {@link CreationPolicy} annotated methods.
 *
 * @author Marc Gathier
 * @since 4.3
 */
public class MethodCreationPolicyDefinition implements HandlerEnhancerDefinition {

    @Override
    public <T> MessageHandlingMember<T> wrapHandler(MessageHandlingMember<T> original) {
        return original.<AggregateCreationPolicy>attribute(HandlerAttributes.AGGREGATE_CREATION_POLICY)
                       .map(creationPolicy -> (MessageHandlingMember<T>) new MethodCreationPolicyHandlingMember<>(
                               original, creationPolicy
                       ))
                       .orElse(original);
    }

    private static class MethodCreationPolicyHandlingMember<T>
            extends WrappedMessageHandlingMember<T>
            implements CreationPolicyMember<T> {

        private final AggregateCreationPolicy creationPolicy;

        private MethodCreationPolicyHandlingMember(MessageHandlingMember<T> delegate,
                                                   AggregateCreationPolicy creationPolicy) {
            super(delegate);
            this.creationPolicy = creationPolicy;
        }

        @Override
        public AggregateCreationPolicy creationPolicy() {
            return creationPolicy;
        }
    }
}
