/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

/**
 * Event scheduling carried over from Axon Framework 4, to keep events that were scheduled before migrating firing, and
 * to cancel them, while a project moves off this API. New code schedules with a scheduler of choice that publishes
 * through the {@link org.axonframework.messaging.eventhandling.gateway.EventGateway}, as the
 * <a href="https://docs.axoniq.io/message-scheduling-guide">Message Scheduling guide</a> describes.
 */
@NullMarked
package org.axonframework.eventhandling.scheduling;

import org.jspecify.annotations.NullMarked;
