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

import io.axoniq.framework.tracing.micrometer.NoOpSpanFactory;
import io.micrometer.context.ContextRegistry;
import io.micrometer.context.ContextSnapshotFactory;
import io.micrometer.context.ThreadLocalAccessor;
import io.micrometer.observation.contextpropagation.ObservationThreadLocalAccessor;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.contextpropagation.ObservationAwareSpanThreadLocalAccessor;
import io.micrometer.tracing.propagation.Propagator;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.messaging.tracing.SpanFactory;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests {@link MicrometerThreadLocalContextPropagationConfigurationEnhancer}: when a Micrometer backend is configured it
 * registers a {@link ContextSnapshotFactory} and, when that factory is built, installs the
 * {@link MicrometerSpanThreadLocalAccessor} on the global {@link ContextRegistry} idempotently and after the
 * ServiceLoader-registered {@link ObservationThreadLocalAccessor} (so it defers to observation-scoped spans).
 *
 * @author Mateusz Nowak
 */
class MicrometerThreadLocalContextPropagationConfigurationEnhancerTest {

    @Test
    void registersContextSnapshotFactoryAndSpanThreadLocalAccessorWhenBackendPresent() {
        // given a Micrometer backend and a resolved ContextSnapshotFactory
        AxonConfiguration configuration = buildWithBackend();

        // when the snapshot factory is built (which lazily registers the span thread-local accessor)
        assertThat(configuration.getOptionalComponent(ContextSnapshotFactory.class)).isPresent();
        configuration.getComponent(ContextSnapshotFactory.class);

        // then exactly one span thread-local accessor is present on the global registry
        List<ThreadLocalAccessor<?>> spanAccessors = ContextRegistry.getInstance().getThreadLocalAccessors().stream()
                .filter(accessor -> ObservationAwareSpanThreadLocalAccessor.KEY.equals(accessor.key()))
                .toList();
        assertThat(spanAccessors).singleElement().isInstanceOf(MicrometerSpanThreadLocalAccessor.class);
    }

    @Test
    void spanAccessorIsRegisteredAfterTheObservationThreadLocalAccessor() {
        // given the snapshot factory has been built at least once
        buildWithBackend().getComponent(ContextSnapshotFactory.class);

        // when
        List<ThreadLocalAccessor<?>> accessors = ContextRegistry.getInstance().getThreadLocalAccessors();
        int observationIndex = indexOfKey(accessors, ObservationThreadLocalAccessor.KEY);
        int spanIndex = indexOfKey(accessors, ObservationAwareSpanThreadLocalAccessor.KEY);

        // then the span accessor defers to observation-scoped spans by being registered after it
        assertThat(spanIndex).isGreaterThanOrEqualTo(0);
        assertThat(observationIndex).isGreaterThanOrEqualTo(0);
        assertThat(spanIndex).isGreaterThan(observationIndex);
    }

    @Test
    void failsWhenConfiguredSpanFactoryIsNotMicrometer() {
        // given a non-Micrometer SpanFactory registered by the user (wins over the enhancer's registerIfNotPresent)
        assertThatThrownBy(() -> {
            AxonConfiguration configuration = MessagingConfigurer.create()
                    .componentRegistry(registry -> registry
                            .registerComponent(Tracer.class, c -> Tracer.NOOP)
                            .registerComponent(Propagator.class, c -> Propagator.NOOP)
                            .registerComponent(SpanFactory.class, c -> NoOpSpanFactory.INSTANCE))
                    .build();
            // resolving the snapshot factory triggers the ContextRegistry decorator, which requires a MicrometerSpanFactory
            configuration.getComponent(ContextSnapshotFactory.class);
        }).hasStackTraceContaining("not a MicrometerSpanFactory");
    }

    private static AxonConfiguration buildWithBackend() {
        return MessagingConfigurer.create()
                                  .componentRegistry(registry -> registry
                                          .registerComponent(Tracer.class, c -> Tracer.NOOP)
                                          .registerComponent(Propagator.class, c -> Propagator.NOOP))
                                  .build();
    }

    private static int indexOfKey(List<ThreadLocalAccessor<?>> accessors, Object key) {
        for (int i = 0; i < accessors.size(); i++) {
            if (key.equals(accessors.get(i).key())) {
                return i;
            }
        }
        return -1;
    }
}
