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
 * Connects Axoniq Framework to Apache Kafka.
 * <p>
 * The {@code publishing} package publishes events to topics, {@code streaming} reads topics with pooled streaming
 * event processors, {@code subscribing} consumes topics in a consumer group with subscribing event processors, and
 * {@code configuration} registers sinks and sources as named components. The {@code codec} package defines how events
 * map to records, and {@code common} holds what all of them share.
 */
@NullMarked
package io.axoniq.framework.kafka;

import org.jspecify.annotations.NullMarked;
