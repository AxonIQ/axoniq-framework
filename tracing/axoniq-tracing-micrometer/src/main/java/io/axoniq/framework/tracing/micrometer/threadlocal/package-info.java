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
