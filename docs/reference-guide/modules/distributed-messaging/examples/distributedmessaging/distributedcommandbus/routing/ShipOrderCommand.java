/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package distributedmessaging.distributedcommandbus.routing;

import org.axonframework.messaging.commandhandling.annotation.Command;

// tag::routing-strategy-command[]
@Command(routingKey = "orderId")
public class ShipOrderCommand {

    private final String orderId;
    private final String trackingNumber;

    // constructor, getters
    // end::routing-strategy-command[]

    public ShipOrderCommand(String orderId, String trackingNumber) {
        this.orderId = orderId;
        this.trackingNumber = trackingNumber;
    }

    public String getOrderId() {
        return orderId;
    }

    public String getTrackingNumber() {
        return trackingNumber;
    }
    // tag::routing-strategy-command[]
}
// end::routing-strategy-command[]
