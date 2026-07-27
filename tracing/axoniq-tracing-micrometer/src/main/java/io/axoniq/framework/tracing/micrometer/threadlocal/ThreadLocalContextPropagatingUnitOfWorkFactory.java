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

package io.axoniq.framework.tracing.micrometer.threadlocal;

import io.axoniq.framework.tracing.micrometer.MicrometerSpanFactory;
import io.micrometer.context.ContextSnapshot;
import io.micrometer.context.ContextSnapshotFactory;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.unitofwork.ProcessingLifecycleInterceptor;
import org.axonframework.messaging.core.unitofwork.UnitOfWork;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkConfiguration;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;

import java.util.Objects;
import java.util.function.Function;

/**
 * {@link UnitOfWorkFactory} decorator that bridges thread-bound state — the active trace context, MDC, security context
 * — captured on the (synchronous) dispatching thread into the framework-executed segments of the created
 * {@link UnitOfWork}, which run on the unit of work's work-scheduler threads.
 * <p>
 * On {@link #create}, the caller's thread-locals are captured into a {@link ContextSnapshot} (the dispatching thread is
 * synchronous for commands/queries, so its HTTP server span, MDC, security and tenant context are all valid here). A
 * {@link ProcessingLifecycleInterceptor} is then composed onto the unit of work's configuration; it fires on the
 * thread that runs each phase action and, for the duration of that action:
 * <ol>
 *     <li>restores the captured thread-locals ({@link ContextSnapshot#setThreadLocals()}), and</li>
 *     <li>makes the action's own Axon span (read live via {@link MicrometerSpanFactory#rawSpanFrom} off the
 *     {@link org.axonframework.messaging.core.unitofwork.ProcessingContext}) the current Micrometer span via
 *     {@link Tracer#withSpan(Span)}, layered <em>inside</em> the restored snapshot so the Axon span wins for tracing
 *     while caller MDC/security accessors stay restored.</li>
 * </ol>
 * Both restorations are torn down deterministically and exception-safely through try-with-resources. This is the
 * imperative-edge {@code ThreadLocal} write sanctioned by the tracing constitution; the
 * {@link org.axonframework.messaging.core.unitofwork.ProcessingContext} remains the source of truth for nesting.
 * <p>
 * The per-action span read always reflects whatever is active on the <em>root</em> {@link org.axonframework.messaging.core.unitofwork.ProcessingContext}
 * -- this bridge fires once per phase action for the whole unit of work, so it can never see a per-event handler span
 * that a streaming-processor batch carries only on a branch (see {@code TracingEventHandlingComponent}). For a
 * per-command/query unit of work, that root span legitimately is the handler span for the entire INVOCATION phase (so
 * a user WebClient/JDBC call nests under it and the {@code traceId} appears in handler log lines); for an event-batch
 * unit of work, it is the batch span, so lifecycle-driven work (e.g. the commit-time event-store append) honestly
 * attaches to the batch rather than to whichever event happened to be handled last.
 *
 * @author Mateusz Nowak
 * @since 5.3.0
 */
@Internal
final class ThreadLocalContextPropagatingUnitOfWorkFactory implements UnitOfWorkFactory {

    private final UnitOfWorkFactory delegate;
    private final Tracer tracer;
    private final ContextSnapshotFactory snapshotFactory;

    /**
     * Initializes the decorator.
     *
     * @param delegate        the {@link UnitOfWorkFactory} to delegate creation to
     * @param tracer          the tracer used to make the per-action Axon span current
     * @param snapshotFactory the factory capturing the dispatching thread's context snapshot
     */
    ThreadLocalContextPropagatingUnitOfWorkFactory(UnitOfWorkFactory delegate, Tracer tracer,
                                                   ContextSnapshotFactory snapshotFactory) {
        this.delegate = Objects.requireNonNull(delegate, "delegate may not be null");
        this.tracer = Objects.requireNonNull(tracer, "tracer may not be null");
        this.snapshotFactory = Objects.requireNonNull(snapshotFactory, "snapshotFactory may not be null");
    }

    @Override
    public UnitOfWork create(String identifier,
                             Function<UnitOfWorkConfiguration, UnitOfWorkConfiguration> customization) {
        // Captured on the (synchronous) dispatching thread: caller's TLs are valid here.
        ContextSnapshot snapshot = snapshotFactory.captureAll();

        ProcessingLifecycleInterceptor restore = ProcessingLifecycleInterceptor.intercept((context, action) -> {
            // LIVE per action: whatever is active on the root context for this phase (see class-level documentation).
            Span current = MicrometerSpanFactory.rawSpanFrom(context);
            try (ContextSnapshot.Scope threadLocals = snapshot.setThreadLocals();
                 Tracer.SpanInScope spanScope = current != null ? tracer.withSpan(current) : () -> {
                 }) {
                return action.get();
            }
        });

        return delegate.create(identifier, config -> customization.apply(config).addLifecycleInterceptor(restore));
    }
}
