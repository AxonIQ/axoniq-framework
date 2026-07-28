/*
 * Copyright (c) 2010-2026. Axoniq B.V.
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
 * Micrometer {@link io.micrometer.tracing.propagation.Propagator} carrier over Axon message metadata: the
 * {@code Propagator.Getter}/{@code Propagator.Setter} pair used to extract and inject trace context to and from a
 * message's metadata, so a trace continues across message and service boundaries.
 */
@NullMarked
package io.axoniq.framework.tracing.micrometer.propagator;

import org.jspecify.annotations.NullMarked;
