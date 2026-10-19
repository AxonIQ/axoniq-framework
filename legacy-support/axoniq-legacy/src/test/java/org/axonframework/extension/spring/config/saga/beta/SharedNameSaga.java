/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package org.axonframework.extension.spring.config.saga.beta;

import org.axonframework.modelling.saga.SagaEventHandler;
import org.axonframework.modelling.saga.StartSaga;

/**
 * A Saga sharing its simple name with {@code org.axonframework.extension.spring.config.saga.alpha.SharedNameSaga}.
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
