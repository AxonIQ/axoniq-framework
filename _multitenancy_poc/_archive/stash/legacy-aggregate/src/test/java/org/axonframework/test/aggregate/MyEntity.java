/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */

package org.axonframework.test.aggregate;

import org.axonframework.eventsourcing.annotation.EventSourcingHandler;

/**
 *
 */
public class MyEntity {

    private Integer lastNumber;

    @EventSourcingHandler
    public void handleMyEvent(MyEvent event) {
        lastNumber = event.getSomeValue();
    }

    public Integer getLastNumber() {
        return lastNumber;
    }
}
