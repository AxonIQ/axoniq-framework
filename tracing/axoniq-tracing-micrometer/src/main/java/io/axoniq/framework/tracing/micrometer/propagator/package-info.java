/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

/**
 * Micrometer {@link io.micrometer.tracing.propagation.Propagator} carrier over Axon message metadata: the
 * {@code Propagator.Getter}/{@code Propagator.Setter} pair used to extract and inject trace context to and from a
 * message's metadata, so a trace continues across message and service boundaries.
 */
@NullMarked
package io.axoniq.framework.tracing.micrometer.propagator;

import org.jspecify.annotations.NullMarked;
