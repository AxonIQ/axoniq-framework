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

package org.axonframework.conversion.upcasting.event;

import java.util.stream.Stream;

/**
 * Event upcaster that does nothing.
 *
 * @author Rene de Waele
 * @since 3.0
 */
public enum NoOpEventUpcaster implements EventUpcaster {

    /**
     * Instance of an EventUpcaster that does nothing.
     */
    INSTANCE;

    @Override
    public Stream<IntermediateEventRepresentation> upcast(
            Stream<IntermediateEventRepresentation> intermediateRepresentations) {
        return intermediateRepresentations;
    }
}
