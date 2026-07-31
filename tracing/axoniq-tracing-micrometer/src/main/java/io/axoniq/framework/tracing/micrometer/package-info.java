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
 * Micrometer Tracing binding for the tracing API, wired by {@code MicrometerTracingConfigurationEnhancer}. The
 * {@code propagator} sub-package holds the {@code Propagator} carrier over message metadata (cross-service
 * propagation); the {@code threadlocal} sub-package holds the in-process
 * thread-local trace-context-propagation bridge. The {@code ProcessingContextAccessor} here is the reactive counterpart
 * that exposes the active span from a {@code ProcessingContext} to Micrometer's context propagation.
 */
@NullMarked
package io.axoniq.framework.tracing.micrometer;

import org.jspecify.annotations.NullMarked;
