/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package workflows.publishingevents;

import java.util.Map;

public class Inventory {

    public Map<String, Object> reserve(Map<String, Object> payload) {
        return Map.of("reserved", true);
    }
}
