/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package workflows.errorhandling;

import org.axonframework.messaging.core.unitofwork.ProcessingContext;

import java.util.Map;

public class InventoryService {

    public static Map<String, Object> releaseStock(ProcessingContext context,
                                                   Map<String, Object> payload) {
        return payload;
    }
}
