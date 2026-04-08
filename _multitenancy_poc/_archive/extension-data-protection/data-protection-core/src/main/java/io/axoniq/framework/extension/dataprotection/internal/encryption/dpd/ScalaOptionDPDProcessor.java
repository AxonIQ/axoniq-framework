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
package io.axoniq.framework.extension.dataprotection.internal.encryption.dpd;

import io.axoniq.framework.extension.dataprotection.internal.encryption.core.Operation;

/**
 * Implementation of {@link DPDProcessor} for Scala Option data. Processes the contained value
 * if present through the routing processor.
 *
 * @author Frans van Buul
 */
class ScalaOptionDPDProcessor implements DPDProcessor<scala.Option> {

    private final RoutingDPDProcessor routingDPDProcessor;

    public ScalaOptionDPDProcessor(RoutingDPDProcessor routingDPDProcessor) {
        this.routingDPDProcessor = routingDPDProcessor;
    }

    @Override
    public scala.Option process(scala.Option input, EncryptionContext context, Operation operation) {
        if (input.isDefined()) {
            Object original = input.get();
            Object result = routingDPDProcessor.process(original, context, operation);
            // If the value was replaced (e.g., immutable object), create new Option
            if (result != original) {
                return scala.Option.apply(result);
            }
        }
        return input;
    }
}
