/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package workflows.testingworkflows;

import java.util.ArrayList;
import java.util.List;

public class InMemoryAuditSink implements AuditSink {

    private final List<String> messages = new ArrayList<>();

    @Override
    public void record(String message) {
        messages.add(message);
    }
}
