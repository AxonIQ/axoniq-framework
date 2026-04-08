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

package org.axonframework.modelling.saga.metamodel;

import org.axonframework.messaging.core.interception.annotation.MessageHandlerInterceptorMemberChain;
import org.axonframework.messaging.core.interception.annotation.NoMoreInterceptors;

/**
 * Interface of a factory for a {@link SagaModel} for any given saga type.
 */
public interface SagaMetaModelFactory {

    /**
     * Create a saga meta model for the given {@code sagaType}. The meta model will inspect the capabilities and
     * characteristics of the given type.
     *
     * @param sagaType The saga class to be inspected
     * @param <T>      The saga type
     * @return Model describing the capabilities and characteristics of the inspected saga class
     */
    <T> SagaModel<T> modelOf(Class<T> sagaType);

    /**
     * Returns an Interceptor Chain of annotated interceptor methods for the given {@code sagaType}. The given chain
     * will invoke all relevant interceptors in an order defined by the handler definition.
     *
     * @param sagaType The saga class to be inspected
     * @param <T>      The saga type
     * @return an interceptor chain that invokes the interceptor handlers
     */
    default <T> MessageHandlerInterceptorMemberChain<T> chainedInterceptor(Class<T> sagaType) {
        return NoMoreInterceptors.instance();
    }
}
