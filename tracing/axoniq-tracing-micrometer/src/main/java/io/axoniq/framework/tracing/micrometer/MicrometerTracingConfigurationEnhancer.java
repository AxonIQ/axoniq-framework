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

package io.axoniq.framework.tracing.micrometer;

import io.micrometer.context.ContextRegistry;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import org.axonframework.common.annotation.RegistrationScope;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.axonframework.messaging.tracing.SpanFactory;
import org.axonframework.messaging.tracing.attributes.SpanAttributesProviderRegistry;
import org.axonframework.messaging.tracing.configuration.TracingConfigurationOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link ConfigurationEnhancer} that enables Micrometer-Tracing-backed tracing. Discovered automatically via
 * ServiceLoader, so having the {@code axoniq-tracing-micrometer} module on the classpath is enough; there is no need to
 * register it manually.
 * <p>
 * It registers a {@link MicrometerSpanFactory} as the {@link SpanFactory} component and installs the
 * {@link ProcessingContextAccessor} (the reactive bridge that exposes the active Axon span from a
 * {@code ProcessingContext} to Micrometer's context propagation).
 * <p>
 * <b>Bridge-specific components (host-supplied).</b> This enhancer resolves the Micrometer abstractions as framework
 * <em>components</em> rather than creating them, because they are provided by a concrete Micrometer tracing bridge (for
 * example {@code micrometer-tracing-bridge-otel}), not by this module:
 * <ul>
 *     <li>{@link Tracer} and {@link Propagator} -- <b>required to enable tracing</b>. When either is absent, this
 *     enhancer backs off entirely (an {@code INFO} log explains how to enable tracing) and no {@link SpanFactory} is
 *     registered -- matching the framework contract that tracing is off when no {@code SpanFactory} component exists.
 *     The jar being on the classpath (possibly transitively) therefore never breaks a configuration that does not
 *     wire a Micrometer bridge.</li>
 *     <li>{@link ObservationRegistry} -- optional; defaulted to {@link ObservationRegistry#create()} when absent.</li>
 * </ul>
 * The host application registers the {@link Tracer} and {@link Propagator} as components, obtained from its chosen
 * bridge, e.g.:
 * <pre>{@code
 * configurer.componentRegistry(registry -> registry
 *         .registerComponent(Tracer.class, c -> tracer)
 *         .registerComponent(Propagator.class, c -> propagator));
 * }</pre>
 * <p>
 * The {@link ContextRegistry} is registered as a component (defaulting to {@link ContextRegistry#getInstance()}), used
 * as the source for the {@code ContextSnapshotFactory} of the thread-local bridge. The {@link ProcessingContextAccessor}
 * is registered onto that component via a decorator (not onto the global singleton directly), so it honours a
 * user-registered {@link ContextRegistry}. The in-process thread-local bridge itself lives in the {@code threadlocal}
 * sibling package and is installed by a separate, independently toggleable enhancer.
 *
 * @author Mateusz Nowak
 * @since 5.3.0
 */
@RegistrationScope("Register the SpanFactory and context-propagation registries once at the root; the registrations "
        + "are idempotent and need not be repeated in child module registries.")
public final class MicrometerTracingConfigurationEnhancer implements ConfigurationEnhancer {

    private static final Logger logger = LoggerFactory.getLogger(MicrometerTracingConfigurationEnhancer.class);

    /**
     * Decorator order for the {@link ContextRegistry} decorator that installs the {@link ProcessingContextAccessor}.
     * Its value is immaterial: the decorator only augments the registry's accessor list, and the accessor it adds is a
     * {@code ContextAccessor}, independent of the thread-local accessors any other decorator contributes.
     */
    private static final int CONTEXT_REGISTRY_DECORATOR_ORDER = 0;

    @Override
    public void enhance(ComponentRegistry registry) {
        registry.registerIfNotPresent(ObservationRegistry.class, c -> ObservationRegistry.create());
        registry.registerIfNotPresent(ContextRegistry.class, c -> ContextRegistry.getInstance());
        registry.registerDecorator(
                ContextRegistry.class,
                CONTEXT_REGISTRY_DECORATOR_ORDER,
                (config, name, contextRegistry) -> {
                    registerProcessingContextAccessor(contextRegistry);
                    return contextRegistry;
                });
        if (registry.hasComponent(Tracer.class) && registry.hasComponent(Propagator.class)) {
            registry.registerIfNotPresent(
                    SpanFactory.class,
                    config -> new MicrometerSpanFactory(
                            requireBridgeComponent(config, Tracer.class),
                            requireBridgeComponent(config, Propagator.class),
                            config.getComponent(SpanAttributesProviderRegistry.class).providers(config)));
        } else if (!registry.hasComponent(SpanFactory.class)) {
            logger.info("The Micrometer tracing binding is on the classpath, but no Tracer and Propagator components "
                                + "are registered -- tracing stays disabled (no SpanFactory). Register both, obtained "
                                + "from a Micrometer tracing bridge (for example micrometer-tracing-bridge-otel), to "
                                + "enable it.");
        }
    }

    /**
     * Resolves a bridge-provided component, failing with an actionable message when it is absent. Because this enhancer
     * is active (its module is on the classpath and it has not been disabled), the host is expected to have provided a
     * Micrometer tracing bridge and registered the {@link Tracer} and {@link Propagator} it exposes.
     *
     * @param config the configuration to resolve the component from
     * @param type   the bridge component type to require
     * @param <C>    the component type
     * @return the resolved component, never {@code null}
     * @throws IllegalStateException if no component of {@code type} is available
     */
    private static <C> C requireBridgeComponent(Configuration config, Class<C> type) {
        return config.getOptionalComponent(type).orElseThrow(() -> new IllegalStateException(
                "Micrometer tracing is enabled but no " + type.getSimpleName() + " component is available. Register a "
                        + "Tracer and a Propagator obtained from a Micrometer tracing bridge (for example "
                        + "micrometer-tracing-bridge-otel), or disable MicrometerTracingConfigurationEnhancer."));
    }

    /**
     * Registers the {@link ProcessingContextAccessor} on the given {@code contextRegistry} when not already present.
     * This reactive bridge is added to the configured {@link ContextRegistry} component; it only becomes functional once
     * the thread-local span accessor is also registered (by the thread-local bridge enhancer).
     */
    private static void registerProcessingContextAccessor(ContextRegistry contextRegistry) {
        boolean alreadyRegistered = contextRegistry.getContextAccessors().stream()
                                                   .anyMatch(accessor -> accessor instanceof ProcessingContextAccessor);
        if (!alreadyRegistered) {
            contextRegistry.registerContextAccessor(new ProcessingContextAccessor());
        }
    }

    @Override
    public int order() {
        return TracingConfigurationOrder.TRACING_DEFAULTS_ENHANCER_ORDER;
    }
}
