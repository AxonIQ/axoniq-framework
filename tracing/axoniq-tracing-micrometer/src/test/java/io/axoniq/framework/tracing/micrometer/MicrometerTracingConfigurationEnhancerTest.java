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

package io.axoniq.framework.tracing.micrometer;

import io.micrometer.context.ContextRegistry;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.messaging.tracing.SpanFactory;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests {@link MicrometerTracingConfigurationEnhancer}, the ServiceLoader-discovered entry point: it registers a
 * {@link MicrometerSpanFactory} as the {@link SpanFactory} component only when a {@link Tracer} and {@link Propagator}
 * are available as components, provides the {@link ObservationRegistry} and {@link ContextRegistry} defaults, and
 * installs the reactive {@link ProcessingContextAccessor} on the global {@link ContextRegistry}.
 *
 * @author Mateusz Nowak
 */
class MicrometerTracingConfigurationEnhancerTest {

    @Nested
    class SpanFactoryRegistration {

        @Test
        void registersMicrometerSpanFactoryWhenTracerAndPropagatorPresent() {
            // given a Tracer and Propagator registered as components
            AxonConfiguration configuration = configurationWithBackend();

            // then the enhancer registered a MicrometerSpanFactory and the backing registries
            assertThat(configuration.getComponent(SpanFactory.class)).isInstanceOf(MicrometerSpanFactory.class);
            assertThat(configuration.getOptionalComponent(ObservationRegistry.class)).isPresent();
            assertThat(configuration.getOptionalComponent(ContextRegistry.class)).isPresent();
        }

        @Test
        void backsOffWithoutRegisteringASpanFactoryWhenBackendComponentsAbsent() {
            // given no Tracer/Propagator components, the enhancer backs off entirely: the configuration builds, no
            // SpanFactory component exists (tracing is off per the framework contract), and nothing else this
            // enhancer would contribute is registered either
            AxonConfiguration configuration = MessagingConfigurer.create().build();

            // then
            assertThat(configuration.getOptionalComponent(SpanFactory.class)).isEmpty();
        }
    }

    @Nested
    class ContextAccessorRegistration {

        @Test
        void registersProcessingContextAccessorOnTheContextRegistryComponentIdempotently() {
            // given the ContextRegistry component is resolved (triggering the decorator) across two configurations
            configurationWithBackend().getComponent(ContextRegistry.class);
            ContextRegistry contextRegistry = configurationWithBackend().getComponent(ContextRegistry.class);

            // then exactly one ProcessingContextAccessor is present on the resolved registry
            long processingContextAccessors = contextRegistry.getContextAccessors().stream()
                    .filter(accessor -> accessor instanceof ProcessingContextAccessor)
                    .count();
            assertThat(processingContextAccessors).isEqualTo(1);
        }
    }

    private static AxonConfiguration configurationWithBackend() {
        return MessagingConfigurer.create()
                                  .componentRegistry(registry -> registry
                                          .registerComponent(Tracer.class, c -> Tracer.NOOP)
                                          .registerComponent(Propagator.class, c -> Propagator.NOOP))
                                  .build();
    }
}
