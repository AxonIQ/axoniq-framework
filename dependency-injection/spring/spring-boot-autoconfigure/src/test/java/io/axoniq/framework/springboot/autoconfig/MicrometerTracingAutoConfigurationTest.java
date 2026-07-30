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

package io.axoniq.framework.springboot.autoconfig;

import io.axoniq.framework.tracing.micrometer.MicrometerSpanFactory;
import io.micrometer.context.ContextSnapshotFactory;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.messaging.tracing.SpanFactory;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link MicrometerTracingAutoConfiguration}. The Micrometer binding self-wires via ServiceLoader; this
 * autoconfiguration only contributes property-driven toggles: it disables both enhancers when {@code axon.tracing} is
 * disabled, and disables only the thread-local bridge when {@code axon.tracing.thread-local-context-propagation} is
 * disabled (leaving core tracing on). The {@link Tracer}/{@link Propagator} components stand in for the beans Spring
 * Boot's tracing auto-configuration would bridge into the registry.
 *
 * @author Mateusz Nowak
 */
@SuppressWarnings("java:S2187")
class MicrometerTracingAutoConfigurationTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(MicrometerTracingAutoConfiguration.class));

    /**
     * Registers a {@link Tracer} and {@link Propagator} as components (mirroring the beans Spring bridges into the
     * registry), so the ServiceLoader-discovered enhancers have a backend to wire.
     */
    private static final Consumer<ComponentRegistry> WITH_BACKEND = registry -> registry
            .registerComponent(Tracer.class, c -> Tracer.NOOP)
            .registerComponent(Propagator.class, c -> Propagator.NOOP);

    @Nested
    class TracingDisabling {

        @Test
        void disablingEnhancerAbsentByDefault() {
            contextRunner.run(context -> assertThat(context).doesNotHaveBean("micrometerTracingDisablingEnhancer"));
        }

        @Test
        void disablesBothEnhancersWhenTracingDisabled() {
            contextRunner.withPropertyValues("axon.tracing.enabled=false").run(context -> {
                ConfigurationEnhancer enhancer =
                        context.getBean("micrometerTracingDisablingEnhancer", ConfigurationEnhancer.class);
                AxonConfiguration configuration = MessagingConfigurer.create()
                        .componentRegistry(registry -> {
                            WITH_BACKEND.accept(registry);
                            enhancer.enhance(registry);
                        })
                        .build();

                // both enhancers disabled -> no SpanFactory and no thread-local ContextSnapshotFactory
                assertThat(configuration.getOptionalComponent(SpanFactory.class))
                        .satisfiesAnyOf(
                                optional -> assertThat(optional).isEmpty(),
                                optional -> assertThat(optional.get()).isNotInstanceOf(MicrometerSpanFactory.class)
                        );
                assertThat(configuration.getOptionalComponent(ContextSnapshotFactory.class)).isEmpty();
            });
        }
    }

    @Nested
    class ThreadLocalContextPropagationToggling {

        @Test
        void bridgeEnabledByDefault() {
            contextRunner.run(context -> {
                ConfigurationEnhancer enhancer = context.getBean("threadLocalContextPropagationTogglingEnhancer",
                                                                  ConfigurationEnhancer.class);
                AxonConfiguration configuration = MessagingConfigurer.create()
                        .componentRegistry(registry -> {
                            WITH_BACKEND.accept(registry);
                            enhancer.enhance(registry);
                        })
                        .build();

                // core tracing on and the thread-local bridge left enabled -> ContextSnapshotFactory present
                assertThat(configuration.getComponent(SpanFactory.class)).isInstanceOf(MicrometerSpanFactory.class);
                assertThat(configuration.getOptionalComponent(ContextSnapshotFactory.class)).isPresent();
            });
        }

        @Test
        void onlyThreadLocalBridgeDisabledWhenPropertyFalse() {
            contextRunner.withPropertyValues("axon.tracing.thread-local-context-propagation.enabled=false")
                         .run(context -> {
                             ConfigurationEnhancer enhancer =
                                     context.getBean("threadLocalContextPropagationTogglingEnhancer",
                                                     ConfigurationEnhancer.class);
                             AxonConfiguration configuration = MessagingConfigurer.create()
                                     .componentRegistry(registry -> {
                                         WITH_BACKEND.accept(registry);
                                         enhancer.enhance(registry);
                                     })
                                     .build();

                             // thread-local bridge disabled (no ContextSnapshotFactory) but core tracing stays on
                             assertThat(configuration.getComponent(SpanFactory.class))
                                     .isInstanceOf(MicrometerSpanFactory.class);
                             assertThat(configuration.getOptionalComponent(ContextSnapshotFactory.class)).isEmpty();
                         });
        }
    }
}
