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
 * Implementation of {@link DPDProcessor} for Scala Iterable data. Processes each element of the iterable
 * through the routing processor.
 *
 * @author Frans van Buul
 */
class ScalaIterableDPDProcessor implements DPDProcessor<scala.collection.Iterable> {

    private final RoutingDPDProcessor routingDPDProcessor;

    public ScalaIterableDPDProcessor(RoutingDPDProcessor routingDPDProcessor) {
        this.routingDPDProcessor = routingDPDProcessor;
    }

    @Override
    public scala.collection.Iterable process(scala.collection.Iterable input, EncryptionContext context, Operation operation) {
        scala.collection.Iterator iterator = input.iterator();
        while(iterator.hasNext()) {
            routingDPDProcessor.process(iterator.next(), context, operation);
        }
        // Note: Scala collections are typically immutable, so we return the input as-is
        // Individual elements are processed, but collection structure remains unchanged
        return input;
    }

}
