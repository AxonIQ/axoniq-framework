/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package workflows.workflowlifecycle;

import org.axonframework.messaging.core.unitofwork.ProcessingContext;

import java.util.Map;

public class NotificationService {

    public static Map<String, Object> notifyCustomer(ProcessingContext context,
                                                     Map<String, Object> payload) {
        return payload;
    }

    public static Map<String, Object> sendReceipt(ProcessingContext context,
                                                  Map<String, Object> payload) {
        return payload;
    }
}
