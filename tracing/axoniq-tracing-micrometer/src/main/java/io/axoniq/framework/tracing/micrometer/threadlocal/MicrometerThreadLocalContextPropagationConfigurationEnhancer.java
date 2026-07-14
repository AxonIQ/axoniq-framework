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
import io.micrometer.context.ContextRegistry;
import io.micrometer.context.ContextSnapshotFactory;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.contextpropagation.ObservationAwareSpanThreadLocalAccessor;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.annotation.RegistrationScope;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.tracing.SpanFactory;
import org.axonframework.messaging.tracing.configuration.TracingConfigurationOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link ConfigurationEnhancer} that installs the Micrometer thread-local trace-context-propagation bridge. Discovered
 * automatically via ServiceLoader, so having the {@code axoniq-tracing-micrometer} module on the classpath is enough.
 * It does two things:
 * <ol>
 *     <li>decorates the {@link UnitOfWorkFactory} with a {@link ThreadLocalContextPropagatingUnitOfWorkFactory}, which
 *     restores the dispatching thread's captured thread-locals (and makes the per-action Axon span current) on the
 *     framework's worker threads; and</li>
 *     <li>registers the {@link ObservationAwareSpanThreadLocalAccessor} on the {@link ContextRegistry} component (the
 *     thread-local counterpart that turns a captured span back into the current span on
 *     {@link io.micrometer.context.ContextSnapshot#setThreadLocals()}).</li>
 * </ol>
 * Together these let instrumented JDBC/gRPC/WebClient calls and MDC logging nest under the active Axon span. Resolution
 * is lazy — deferred to framework-configuration time — so it observes the finally-registered {@code SpanFactory}. The
 * enhancer backs off entirely (with an {@code INFO} log) when no {@link Tracer} component is registered, and the
 * decorators back off when no {@link SpanFactory} component exists at all — the jar being on the classpath never breaks
 * a configuration that does not wire tracing. If a {@code SpanFactory} <em>is</em> configured but is a different
 * implementation than {@link MicrometerSpanFactory}, an {@link IllegalStateException} is raised rather than silently
 * skipping — configure the Micrometer {@code SpanFactory} or disable this bridge.
 * <p>
 * This governs only the <em>in-process, thread-local</em> bridge. Cross-service trace-context propagation (via the
 * {@code Propagator} over message metadata) is part of core tracing and is unaffected by disabling this enhancer. To
 * turn this bridge off, disable this enhancer: call {@link ComponentRegistry#disableEnhancer(Class)} with this class
 * (plain-Java setups), or set {@code axon.tracing.thread-local-context-propagation.enabled=false} in Spring Boot, whose
 * auto-configuration disables this enhancer for you.
 * <p>
 * The {@link ContextSnapshotFactory} that captures the dispatching thread's thread-locals is registered as a component
 * via {@link ComponentRegistry#registerIfNotPresent} (built from the {@link ContextRegistry} component), so a
 * user-registered factory takes precedence.
 *
 * @author Mateusz Nowak
 * @since 5.3.0
 */
@Internal
@RegistrationScope("Register the UnitOfWorkFactory decorator once at the root; do not re-invoke in child module "
        + "registries (the DecoratorDefinition is copied down and reaches module-built factories on its own).")
public final class MicrometerThreadLocalContextPropagationConfigurationEnhancer implements ConfigurationEnhancer {

    private static final Logger logger =
            LoggerFactory.getLogger(MicrometerThreadLocalContextPropagationConfigurationEnhancer.class);

    /**
     * Decorator order for the thread-local context-propagation decorator. A low value keeps it near the delegate; its
     * position relative to other {@code UnitOfWorkFactory} decorators is immaterial because it only composes an action
     * interceptor onto the created unit of work's configuration.
     */
    public static final int DECORATOR_ORDER = 0;

    @Override
    public void enhance(ComponentRegistry registry) {
        if (!registry.hasComponent(Tracer.class)) {
            logger.info("The Micrometer thread-local context-propagation bridge is on the classpath, but no Tracer "
                                + "component is registered -- the bridge stays disabled. Register a Tracer obtained "
                                + "from a Micrometer tracing bridge (for example micrometer-tracing-bridge-otel) to "
                                + "enable it.");
            return;
        }
        // Idempotent with the core enhancer's registration: this enhancer must stay self-sufficient, as enhancer
        // execution order between the two is unspecified and the core enhancer may back off independently.
        registry.registerIfNotPresent(ContextRegistry.class, c -> ContextRegistry.getInstance());
        registry.registerDecorator(
                ContextRegistry.class,
                DECORATOR_ORDER,
                (config, name, contextRegistry) -> {
                    if (micrometerSpanFactoryPresent(config)) {
                        registerSpanThreadLocalAccessor(config, contextRegistry);
                    }
                    return contextRegistry;
                });
        registry.registerIfNotPresent(
                ContextSnapshotFactory.class,
                config -> ContextSnapshotFactory.builder()
                                                .contextRegistry(config.getComponent(ContextRegistry.class))
                                                .build());
        registry.registerDecorator(
                UnitOfWorkFactory.class,
                DECORATOR_ORDER,
                (config, name, delegate) -> {
                    if (!micrometerSpanFactoryPresent(config)) {
                        return delegate;
                    }
                    return new ThreadLocalContextPropagatingUnitOfWorkFactory(
                            delegate,
                            config.getComponent(Tracer.class),
                            config.getComponent(ContextSnapshotFactory.class)
                    );
                }
        );
    }

    /**
     * Registers the {@link ObservationAwareSpanThreadLocalAccessor} on {@code contextRegistry} when not already present.
     * This is the thread-local accessor that restores a captured span as the current span on
     * {@link io.micrometer.context.ContextSnapshot#setThreadLocals()}; it is added through a {@link ContextRegistry}
     * decorator, applied when that component is resolved (when the {@link ContextSnapshotFactory} is built), so it can
     * read the {@code Tracer} from the configured components.
     */
    private static void registerSpanThreadLocalAccessor(Configuration config, ContextRegistry contextRegistry) {
        Object spanAccessorKey = ObservationAwareSpanThreadLocalAccessor.KEY;
        boolean alreadyRegistered = contextRegistry.getThreadLocalAccessors().stream()
                                                   .anyMatch(accessor -> spanAccessorKey.equals(accessor.key()));
        if (!alreadyRegistered) {
            ObservationRegistry observationRegistry = config.getOptionalComponent(ObservationRegistry.class)
                                                             .orElseGet(ObservationRegistry::create);
            contextRegistry.registerThreadLocalAccessor(
                    new ObservationAwareSpanThreadLocalAccessor(observationRegistry, config.getComponent(Tracer.class)));
        }
    }

    /**
     * Decides whether the bridge can operate against the configured {@link SpanFactory}. When no {@code SpanFactory}
     * component exists at all (tracing is off -- for example the core enhancer backed off for lack of a bridge, or was
     * disabled), the bridge backs off silently: there is no Micrometer span to make thread-local-current. When a
     * {@code SpanFactory} exists but is <b>not</b> a {@link MicrometerSpanFactory}, that is a genuine misconfiguration
     * (a different tracing implementation is wired while this bridge is active) and fails fast rather than silently
     * doing nothing.
     *
     * @param config the configuration to resolve the {@link SpanFactory} from
     * @return {@code true} when a {@link MicrometerSpanFactory} is configured; {@code false} when no
     * {@link SpanFactory} exists at all
     * @throws IllegalStateException if the configured {@link SpanFactory} is not a {@link MicrometerSpanFactory}
     */
    private static boolean micrometerSpanFactoryPresent(Configuration config) {
        SpanFactory spanFactory = config.getOptionalComponent(SpanFactory.class).orElse(null);
        if (spanFactory == null) {
            logger.info("No SpanFactory component is configured -- the Micrometer thread-local context-propagation "
                                + "bridge stays disabled.");
            return false;
        }
        if (!(spanFactory instanceof MicrometerSpanFactory)) {
            throw new IllegalStateException(
                    "The Micrometer thread-local context-propagation bridge is enabled but the configured SpanFactory "
                            + "is a " + spanFactory.getClass().getName() + ", not a MicrometerSpanFactory. Configure the "
                            + "Micrometer SpanFactory, or disable this bridge with "
                            + "axon.tracing.thread-local-context-propagation.enabled=false.");
        }
        return true;
    }

    @Override
    public int order() {
        return TracingConfigurationOrder.TRACING_DEFAULTS_ENHANCER_ORDER;
    }
}
