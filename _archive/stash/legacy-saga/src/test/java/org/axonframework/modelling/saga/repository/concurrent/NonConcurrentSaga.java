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

package org.axonframework.modelling.saga.repository.concurrent;

import org.axonframework.modelling.saga.EndSaga;
import org.axonframework.modelling.saga.SagaEventHandler;
import org.axonframework.modelling.saga.StartSaga;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Allard Buijze
 */
public class NonConcurrentSaga extends AbstractTestSaga {

    private List<Object> events = new ArrayList<>();

    @StartSaga
    @SagaEventHandler(associationProperty = "id")
    public void handleCreated(CreateEvent event) {
        this.events.add(event);
    }

    @SagaEventHandler(associationProperty = "id")
    public void handleUpdate(UpdateEvent event) {
        this.events.add(event);
    }

    @EndSaga
    @SagaEventHandler(associationProperty = "id")
    public void handleDelete(DeleteEvent event) {
        this.events.add(event);
    }

    @Override
    public List<Object> getEvents() {
        return events;
    }
}
