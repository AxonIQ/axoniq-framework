/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

/**
 * Micrometer Tracing binding for the tracing API, wired by {@code MicrometerTracingConfigurationEnhancer}. The
 * {@code propagator} sub-package holds the {@code Propagator} carrier over message metadata (cross-service
 * propagation); the {@code threadlocal} sub-package holds the in-process
 * thread-local trace-context-propagation bridge. The {@code ProcessingContextAccessor} here is the reactive counterpart
 * that exposes the active span from a {@code ProcessingContext} to Micrometer's context propagation.
 */
@NullMarked
package io.axoniq.framework.tracing.micrometer;

import org.jspecify.annotations.NullMarked;
