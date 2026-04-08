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

package org.axonframework.modelling.command.inspection;

import java.util.Collections;
import java.util.Set;

/**
 * Interface of a factory for an {@link AggregateModel} for any given type defining an aggregate.
 */
public interface AggregateMetaModelFactory {

    /**
     * Create an Aggregate meta model for the given {@code aggregateType}. The meta model will inspect the capabilities
     * and characteristics of the given type.
     *
     * @param aggregateType The Aggregate class to be inspected
     * @param <T>           The Aggregate type
     * @return Model describing the capabilities and characteristics of the inspected Aggregate class
     */
    default <T> AggregateModel<T> createModel(Class<T> aggregateType) {
        return createModel(aggregateType, Collections.emptySet());
    }

    /**
     * Create an Aggregate meta model for the given {@code aggregateType} and provided {@code subtypes}. The meta model
     * will inspect the capabilities and characteristics of the given {@code aggregateType} and its {@code subtypes}.
     *
     * @param aggregateType The Aggregate class to be inspected
     * @param subtypes      Subtypes of this Aggregate class
     * @param <T>           The Aggregate type
     * @return Model describing the capabilities and characteristics of the inspected Aggregate class and its subtypes
     */
    <T> AggregateModel<T> createModel(Class<T> aggregateType, Set<Class<? extends T>> subtypes);
}
