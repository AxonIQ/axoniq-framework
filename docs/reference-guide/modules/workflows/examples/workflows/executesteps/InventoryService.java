/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package workflows.executesteps;

import java.util.Map;

public class InventoryService {

    public static boolean reserveStock() {
        return true;
    }

    public static boolean reserveStock(Map<String, Object> input) {
        return true;
    }
}
