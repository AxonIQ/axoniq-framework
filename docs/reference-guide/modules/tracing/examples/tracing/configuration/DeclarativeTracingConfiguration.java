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

package tracing.configuration;

import io.axoniq.framework.messaging.distributed.tracing.DistributedTracingSettings;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import org.axonframework.eventsourcing.tracing.configuration.EventSourcingTracingSettings;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.messaging.tracing.LoggingSpanFactory;
import org.axonframework.messaging.tracing.SpanFactory;
import org.axonframework.messaging.tracing.configuration.MessagingTracingSettings;
import org.axonframework.modelling.tracing.configuration.ModellingTracingSettings;

public final class DeclarativeTracingConfiguration {

    // tag::register-tracing[]
    public MessagingConfigurer registerTracing(MessagingConfigurer configurer,
                                                Tracer tracer,
                                                Propagator propagator) {
        return configurer.componentRegistry(registry -> registry
                .registerComponent(Tracer.class, configuration -> tracer)
                .registerComponent(Propagator.class, configuration -> propagator));
    }
    // end::register-tracing[]

    // tag::logging-span-factory[]
    public MessagingConfigurer registerLoggingSpanFactory(MessagingConfigurer configurer) {
        return configurer.componentRegistry(registry -> registry.registerComponent(
                SpanFactory.class,
                configuration -> LoggingSpanFactory.INSTANCE
        ));
    }
    // end::logging-span-factory[]

    // tag::environment[]
    public MessagingConfigurer configureTracingForEnvironment(MessagingConfigurer configurer,
                                                               boolean tracingEnabled,
                                                               Tracer tracer,
                                                               Propagator propagator) {
        return tracingEnabled ? registerTracing(configurer, tracer, propagator) : configurer;
    }
    // end::environment[]

    // tag::component-settings[]
    public MessagingConfigurer configureTracingComponents(MessagingConfigurer configurer) {
        return configurer.componentRegistry(registry -> registry
                .registerComponent(
                        MessagingTracingSettings.class,
                        configuration -> MessagingTracingSettings.enabledByDefault()
                                .withEventProcessorBatchTraceEnabled(false)
                                .withQueryBusEnabled(false)
                )
                .registerComponent(
                        ModellingTracingSettings.class,
                        configuration -> ModellingTracingSettings.enabledByDefault()
                                .withRepositoryEnabled(false)
                )
                .registerComponent(
                        EventSourcingTracingSettings.class,
                        configuration -> EventSourcingTracingSettings.enabledByDefault()
                                .withSnapshotStoreEnabled(false)
                )
                .registerComponent(
                        DistributedTracingSettings.class,
                        configuration -> DistributedTracingSettings.enabledByDefault()
                                .withQueryBusConnectorEnabled(false)
                ));
    }
    // end::component-settings[]
}
