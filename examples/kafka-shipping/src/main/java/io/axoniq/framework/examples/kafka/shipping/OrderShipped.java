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

package io.axoniq.framework.examples.kafka.shipping;

import org.axonframework.messaging.eventhandling.annotation.Event;

/**
 * An event on the inside: the shipping service records it, and its shape evolves with the service.
 *
 * @param orderId            the order that shipped
 * @param parcelId           the parcel the order shipped in
 * @param destinationCountry the country the parcel is sent to
 * @param international      whether the parcel crosses a border
 */
@Event(namespace = "shipping", name = "OrderShipped", version = "3.2")
public record OrderShipped(String orderId, String parcelId, String destinationCountry, boolean international) {

}
