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

import org.axonframework.conversion.upcasting.SingleEntryMultiUpcaster;

/**
 * Abstract implementation of a {@link SingleEntryMultiUpcaster} and an {@link EventUpcaster} that eases the common
 * process of upcasting one intermediate event representation to several other representations by applying a flat
 * mapping function to the input stream of intermediate representations.
 *
 * @author Steven van Beelen
 * @since 3.0.6
 */
public abstract class EventMultiUpcaster
        extends SingleEntryMultiUpcaster<IntermediateEventRepresentation> implements EventUpcaster {

}
