/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package workflows.steporchestration;

import org.axonframework.messaging.core.unitofwork.ProcessingContext;

import java.util.Map;

public class InventoryService {

    public static boolean reserveStock() {
        return true;
    }

    public static Map<String, Object> reserveStock(ProcessingContext context,
                                                   Map<String, Object> payload) {
        return payload;
    }
}
