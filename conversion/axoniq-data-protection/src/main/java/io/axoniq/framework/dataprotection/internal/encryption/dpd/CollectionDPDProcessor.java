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
package io.axoniq.framework.dataprotection.internal.encryption.dpd;

import io.axoniq.framework.dataprotection.internal.encryption.core.Operation;

import java.util.Collection;

/**
 * Implementation of {@link DPDProcessor} for Collection data. Processes each element of the collection
 * through the routing processor.
 *
 * @author Frans van Buul
 */
class CollectionDPDProcessor implements DPDProcessor<Collection> {

    private final RoutingDPDProcessor routingDPDProcessor;

    public CollectionDPDProcessor(RoutingDPDProcessor routingDPDProcessor) {
        this.routingDPDProcessor = routingDPDProcessor;
    }

    @Override
    public Collection process(Collection input, EncryptionContext context, Operation operation) {
        // For collections, we process items in place
        // If any items are immutable (like records), they'll be replaced
        java.util.List<Object> itemsList = new java.util.ArrayList<>(input);
        boolean changed = false;

        for (int i = 0; i < itemsList.size(); i++) {
            Object item = itemsList.get(i);
            Object result = routingDPDProcessor.process(item, context, operation);
            if (item != result) {
                itemsList.set(i, result);
                changed = true;
            }
        }

        if (changed) {
            input.clear();
            input.addAll(itemsList);
        }

        return input;
    }

}
