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

package org.axonframework.messaging.core;

/**
 * Utility class to obtain resources used in previous versions of Axon Framework.
 *
 * @author Allard Buijze
 * @since 5.0.0
 */
public abstract class LegacyResources {

    /**
     * The ResourceKey to obtain the Aggregate Identifier from an Event's context.
     */
    public static final Context.ResourceKey<String> AGGREGATE_IDENTIFIER_KEY = Context.ResourceKey.withLabel("aggregateIdentifier");
    /**
     * The ResourceKey to obtain the Aggregate Type from an Event's context.
     */
    public static final Context.ResourceKey<String> AGGREGATE_TYPE_KEY = Context.ResourceKey.withLabel("aggregateType");
    /**
     * The ResourceKey to obtain the Aggregate Sequence Number from an Event's context.
     */
    public static final Context.ResourceKey<Long> AGGREGATE_SEQUENCE_NUMBER_KEY = Context.ResourceKey.withLabel("aggregateSequenceNumber");
}
