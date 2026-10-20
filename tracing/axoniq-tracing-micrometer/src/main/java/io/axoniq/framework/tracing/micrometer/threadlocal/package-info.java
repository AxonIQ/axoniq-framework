/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

/**
 * In-process, thread-local trace-context propagation for the Micrometer binding: captures the dispatching thread's
 * thread-bound state (the active span, MDC, security context) and restores it on the framework's worker threads, so the
 * active Axon span is thread-local-current inside handler execution and instrumented JDBC/gRPC/WebClient calls and MDC
 * logging nest under it.
 * <p>
 * This is distinct from cross-service propagation, which is carried by the {@code Propagator} over message metadata.
 * The bridge can be disabled independently through its configuration enhancer.
 */
@NullMarked
package io.axoniq.framework.tracing.micrometer.threadlocal;

import org.jspecify.annotations.NullMarked;
