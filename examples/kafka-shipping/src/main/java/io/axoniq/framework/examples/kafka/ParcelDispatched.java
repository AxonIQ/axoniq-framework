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

package io.axoniq.framework.examples.kafka;

import org.axonframework.messaging.eventhandling.annotation.Event;

/**
 * An event on the outside: the contract the shipping service publishes to other systems through Kafka.
 *
 * @param parcelId           the parcel that was dispatched
 * @param destinationCountry the country the parcel is sent to
 */
@Event(namespace = "parcels", name = "ParcelDispatched", version = "1.0")
public record ParcelDispatched(String parcelId, String destinationCountry) {

}
