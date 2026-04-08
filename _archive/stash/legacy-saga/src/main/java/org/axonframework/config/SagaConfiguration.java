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

package org.axonframework.config;

import org.axonframework.messaging.eventhandling.processing.errorhandling.ListenerInvocationErrorHandler;
import org.axonframework.modelling.saga.AbstractSagaManager;
import org.axonframework.modelling.saga.SagaRepository;
import org.axonframework.modelling.saga.repository.SagaStore;

/**
 * Represents a set of components needed to configure a Saga.
 *
 * @param <S> a generic specifying the Saga type
 * @author Milan Savic
 * @since 4.0
 */
public interface SagaConfiguration<S> {

    /**
     * Gets the Saga Type.
     *
     * @return the Saga Type
     */
    Class<S> type();

    /**
     * Retrieve the Saga Manager in this Configuration.
     *
     * @return the Manager for this Saga Configuration
     */
    AbstractSagaManager<S> manager();

    /**
     * Retrieve the {@link SagaRepository} in this Configuration.
     *
     * @return the {@link SagaRepository} in this Configuration
     */
    SagaRepository<S> repository();

    /**
     * Retrieve the {@link SagaStore} in this Configuration.
     *
     * @return the {@link SagaStore} in this Configuration
     */
    SagaStore<? super S> store();

    /**
     * Retrieve the Saga's {@link ListenerInvocationErrorHandler}.
     *
     * @return the Saga's {@link ListenerInvocationErrorHandler}
     */
    ListenerInvocationErrorHandler listenerInvocationErrorHandler();

    /**
     * Gets the Processing Group this Saga is assigned to.
     *
     * @return the Processing Group this Saga is assigned to
     */
    String processingGroup();
}
