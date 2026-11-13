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
 * Publishing events to Kafka topics. A {@link io.axoniq.framework.kafka.publishing.KafkaEventSink} publishes only the
 * events given to it, typically by an event handler that reacts to an internal event.
 */
@NullMarked
package io.axoniq.framework.kafka.publishing;

import org.jspecify.annotations.NullMarked;
