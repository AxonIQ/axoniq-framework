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
 * Implementation of {@link DPDProcessor} for array data. Processes each element of the array
 * through the routing processor.
 *
 * @author Frans van Buul
 */
class ArrayDPDProcessor implements DPDProcessor<Object[]> {

    private final RoutingDPDProcessor routingDPDProcessor;

    public ArrayDPDProcessor(RoutingDPDProcessor routingDPDProcessor) {
        this.routingDPDProcessor = routingDPDProcessor;
    }

    @Override
    public Object[] process(Object[] input, EncryptionContext context, Operation operation) {
        for (int i = 0; i < input.length; i++) {
            Object item = input[i];
            Object result = routingDPDProcessor.process(item, context, operation);
            // If the item changed (e.g., immutable object replaced), update array
            if (item != result) {
                input[i] = result;
            }
        }
        return input;
    }

}
