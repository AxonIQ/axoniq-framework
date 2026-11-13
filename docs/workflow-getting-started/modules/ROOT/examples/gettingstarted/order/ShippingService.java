/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package gettingstarted.order;

import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

// tag::shipping-service[]
public class ShippingService {

    static Logger logger = LoggerFactory.getLogger(ShippingService.class);

    public static Map<String, Object> shipOrder(ProcessingContext pc,
                                                 Map<String, Object> payload) {
        logger.info("Shipping order: {}", payload.get("orderId"));
        return Map.of("shipped", true);
    }
}
// end::shipping-service[]
