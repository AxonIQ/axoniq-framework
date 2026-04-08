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

package org.axonframework.extension.spring.config.annotation;

import org.axonframework.messaging.eventhandling.annotation.EventHandler;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.springframework.transaction.annotation.Transactional;

/**
 * @author Allard Buijze
 */
@Transactional
public class TransactionalListener implements SomeMeaninglessInterface {

    private int invocations;

    @EventHandler
    public void handleEvent(EventMessage event) {
        this.invocations++;
    }

    public int getInvocations() {
        return invocations;
    }
}
