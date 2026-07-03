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
 * Part of the Axon Messaging module. Contains the checkpoint protocol that lets an event-handling unit manage its own
 * progress: it decides when its work for a segment is durable and requests the owning streaming processor to advance
 * the stored {@link org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken}.
 */
@NullMarked
package io.axoniq.framework.messaging.eventstreaming.checkpoint;

import org.jspecify.annotations.NullMarked;
