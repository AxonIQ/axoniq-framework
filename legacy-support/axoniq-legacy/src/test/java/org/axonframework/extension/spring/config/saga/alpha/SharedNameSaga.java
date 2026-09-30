/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 *
 * The software is available for evaluation use without registration.
 * Continued use beyond the evaluation period requires registration
 * and a commercial license. See the License for the specific language
 * governing permissions and limitations under the License.
 * You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at:
 *  https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *  https://www.axoniq.io/pricing
 */

package org.axonframework.extension.spring.config.saga.alpha;

import org.axonframework.modelling.saga.SagaEventHandler;
import org.axonframework.modelling.saga.StartSaga;

/**
 * A Saga sharing its simple name with {@code org.axonframework.extension.spring.config.saga.beta.SharedNameSaga}.
 * <p>
 * The pair exists to verify that two Saga types deriving the same processor name end up on one processor, and that
 * their component names fall back to the fully qualified class name to stay unique.
 *
 * @author Mateusz Nowak
 */
public class SharedNameSaga {

    /**
     * Starts a Saga instance for the given {@code event}.
     *
     * @param event the event starting this Saga
     */
    @StartSaga
    @SagaEventHandler(associationProperty = "id")
    public void on(SagaStarted event) {
        // Intentionally empty; the Saga only needs a handler to be a valid event handling component.
    }

    /**
     * The event starting a {@code SharedNameSaga}.
     *
     * @param id the association value of the Saga instance
     */
    public record SagaStarted(String id) {

    }
}
