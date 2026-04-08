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

/**
 * Specialization of the Module Configuration for modules that define an Aggregate Configuration. This interface allows
 * components to retrieve the Repository used to load Aggregates of the type defined in this Configuration.
 *
 * @param <A> The type of Aggregate defined in this Configuration
 * @author Allard Buijze
 * @since 3.0
 */ // TODO #3486 - Revamp this to a workable ConfigurationEnhancer / Module
public interface AggregateConfiguration<A> /*extends ModuleConfiguration*/ {

//    /**
//     * Returns the repository defined to load instances of the Aggregate type defined in this configuration.
//     *
//     * @return the repository to load aggregates
//     */
//    LegacyRepository<A> repository();
//
//    /**
//     * Returns the type of Aggregate defined in this configuration.
//     *
//     * @return the type of Aggregate defined in this configuration
//     */
//    Class<A> aggregateType();
//
//    /**
//     * Returns the {@link AggregateFactory} defined in this configuration.
//     *
//     * @return the {@link AggregateFactory} defined in this configuration.
//     */
//    AggregateFactory<A> aggregateFactory();
//
//    /**
//     * Returns the {@link SnapshotFilter} defined in this configuration.
//     *
//     * @return the {@link SnapshotFilter} defined in this configuration
//     */
//    SnapshotFilter snapshotFilter();
}
