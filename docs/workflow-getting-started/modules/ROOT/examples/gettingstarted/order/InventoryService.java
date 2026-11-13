/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package gettingstarted.order;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

// tag::inventory-service[]
public class InventoryService {

    static Logger logger = LoggerFactory.getLogger(InventoryService.class);

    public static boolean reserveStock() {
        logger.info("Reserving stock for order.");
        return true;
    }

    public static boolean reserveStock(Map<String, Object> payload) {
        logger.info("Reserving stock for order: {}", payload);
        return true;
    }
}
// end::inventory-service[]
