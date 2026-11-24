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

/**
 * The mapping between events and Kafka records.
 * <p>
 * {@link io.axoniq.framework.kafka.codec.KafkaEventCodec} is the contract that publishers and consumers of a topic
 * agree on. {@link io.axoniq.framework.kafka.codec.DefaultKafkaEventCodec} implements the Axoniq Framework wire format,
 * which writes the event's identity, type and metadata as readable record headers.
 * {@link io.axoniq.framework.kafka.codec.UndecodableRecordPolicy} decides what happens to records that do not carry an
 * event, such as records written by other applications.
 */
@NullMarked
package io.axoniq.framework.kafka.codec;

import org.jspecify.annotations.NullMarked;
