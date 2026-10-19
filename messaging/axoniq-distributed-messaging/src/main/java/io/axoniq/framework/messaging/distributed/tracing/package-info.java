/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

/**
 * Tracing wiring for the distributed bus connectors ({@code CommandBusConnector}, {@code QueryBusConnector}). The
 * delegating tracing decorators live in the connector-owned {@code tracing} packages; they are registered with the
 * framework by the ServiceLoader-discovered {@code DistributedTracingConfigurationEnhancer}.
 */
@NullMarked
package io.axoniq.framework.messaging.distributed.tracing;

import org.jspecify.annotations.NullMarked;
