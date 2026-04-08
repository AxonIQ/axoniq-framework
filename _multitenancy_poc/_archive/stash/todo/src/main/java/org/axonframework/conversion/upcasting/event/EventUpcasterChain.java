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

import org.axonframework.conversion.upcasting.GenericUpcasterChain;

import java.util.List;

/**
 * Upcaster chain used to upcast {@link IntermediateEventRepresentation event representations}.
 * <p/>
 * Upcasters expecting different serialized object types may be merged into a single chain, as long as the order of
 * related upcasters can be guaranteed.
 *
 * @author Rene de Waele
 * @since 3.0
 */
public class EventUpcasterChain extends GenericUpcasterChain<IntermediateEventRepresentation> implements EventUpcaster {

    /**
     * Initializes an upcaster chain from one or more upcasters.
     *
     * @param upcasters the upcasters to chain
     */
    public EventUpcasterChain(EventUpcaster... upcasters) {
        super(upcasters);
    }

    /**
     * Initializes an upcaster chain from the given list of upcasters.
     *
     * @param upcasters the upcasters to chain
     */
    public EventUpcasterChain(List<? extends EventUpcaster> upcasters) {
        super(upcasters);
    }
}
