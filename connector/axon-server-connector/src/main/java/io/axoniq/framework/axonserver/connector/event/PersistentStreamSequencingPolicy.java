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
package io.axoniq.framework.axonserver.connector.event;

import org.axonframework.messaging.core.sequencing.FullConcurrencyPolicy;
import org.axonframework.messaging.core.sequencing.PropertySequencingPolicy;
import org.axonframework.messaging.core.sequencing.SequentialPerAggregatePolicy;
import org.axonframework.messaging.core.sequencing.SequentialPolicy;

/**
 * Interface defining the available persistent stream sequencing policies as string constants.
 *
 * @author Jakob Hatzl
 * @since 5.2.0
 */
public interface PersistentStreamSequencingPolicy {

    /**
     * A {@link String} constant representing the "sequential per aggregate" sequencing policy. This means all events
     * belonging to the same aggregate are handled sequentially. The behavior of this policy resembles the
     * {@link SequentialPerAggregatePolicy}.
     */
    String SEQUENTIAL_PER_AGGREGATE_POLICY = "SequentialPerAggregatePolicy";

    /**
     * A {@link String} constant representing the "metadata" sequencing policy. The policy utilizes values present in
     * the metadata of an event to define the sequence identifier.
     */
    String METADATA_SEQUENCING_POLICY = "MetadataSequencingPolicy";

    /**
     * A {@link String} constant representing the sequential policy. This means all events are handled sequentially. The
     * behavior of this policy resembles the {@link SequentialPolicy}.
     */
    String SEQUENTIAL_POLICY = "SequentialPolicy";

    /**
     * A {@link String} constant representing the full concurrency policy. This means all events are spread out over the
     * available segments, regardless of the sequence identifier. The behavior of this policy resembles the
     * {@link FullConcurrencyPolicy}.
     */
    String FULL_CONCURRENCY_POLICY = "FullConcurrencyPolicy";

    /**
     * A {@link String} constant representing the property sequencing policy. This policy retrieves a value from the
     * event's payload to decide the sequence identifier of the event. The behavior of this policy resembles the
     * {@link PropertySequencingPolicy}.
     */
    String PROPERTY_SEQUENCING_POLICY = "PropertySequencingPolicy";
}
