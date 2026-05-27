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

package io.axoniq.framework.tracing;

import org.axonframework.messaging.core.unitofwork.ProcessingContext;

import java.util.Collection;
import java.util.Map;

/**
 * Contract for propagating a tracing context across process boundaries through a message's metadata, using a
 * text-map representation (for the OpenTelemetry binding, the W3C Trace Context {@code traceparent} / {@code
 * tracestate} entries).
 * <p>
 * This is the lower-level building block underneath {@link Span#propagateContext(org.axonframework.messaging.core.Message)}
 * and is exposed publicly so external decorator authors can propagate context onto messages that the built-in
 * decorators do not handle. It never reads a thread-bound "current span": {@link #inject(ProcessingContext)} renders
 * the span that is <em>active on the given {@link ProcessingContext}</em> (see {@link Span#start()}) as metadata
 * entries. {@link #fields()} reports the reserved metadata keys this propagator owns, so callers can detect and
 * document collisions with user metadata.
 *
 * @author Mateusz Nowak
 * @since 5.2.0
 */
public interface MetadataContextPropagator {

    /**
     * Renders the tracing context that is active on the given {@code context} as metadata entries to merge into an
     * outbound message. Returns an empty map when no span is active on the context; never {@code null} and never reads
     * a thread-local.
     *
     * @param context the processing context whose active span's context is rendered
     * @return the propagation metadata entries for the context's active span, possibly empty
     */
    Map<String, String> inject(ProcessingContext context);

    /**
     * Returns the reserved metadata keys this propagator reads from and writes to (for the OpenTelemetry binding,
     * {@code traceparent} and {@code tracestate}). Useful for detecting and documenting collisions with user metadata.
     *
     * @return the reserved propagation metadata keys
     */
    Collection<String> fields();
}
