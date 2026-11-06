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
package io.axoniq.framework.kafka.subscribing;

/**
 * Where a {@link KafkaSubscribableEventSource} starts consuming a partition for which its consumer group has no
 * committed offset: every partition of a new consumer group, and partitions that are added to a topic later.
 * <p>
 * The initial position is independent of the {@link io.axoniq.framework.kafka.common.RetentionGapPolicy}, which
 * decides what happens when a committed offset no longer exists. Configure it with
 * {@link KafkaSubscribableEventSourceConfiguration#withInitialPosition(InitialPosition)}.
 *
 * @author Allard Buijze
 * @since 5.5.0
 */
public enum InitialPosition {

    /**
     * Start at the earliest record still available in the partition, so that the group processes the topic's
     * history. This is the default.
     */
    EARLIEST,

    /**
     * Start at the end of the partition, so that the group only processes records published from then on. A
     * partition added to a topic later also starts at its end, once the group first picks it up: records published
     * to it before then are not processed.
     * <p>
     * The source commits that end position right away, so a rebalance or restart before the first record is processed
     * continues from it.
     */
    LATEST
}
