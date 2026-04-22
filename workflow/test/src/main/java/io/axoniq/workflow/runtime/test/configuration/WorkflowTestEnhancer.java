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
package io.axoniq.workflow.runtime.test.configuration;

import io.axoniq.workflow.runtime.test.utils.DelayedPublisher;
import jakarta.annotation.Nonnull;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.axonframework.conversion.Converter;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.eventsourcing.eventstore.InterceptingEventStore;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.eventhandling.EventSink;

import java.util.concurrent.Executor;

import static io.axoniq.workflow.configuration.WorkflowEnhancer.WORKFLOW_ENGINE_EXECUTOR;

/**
 * Test enhancer for workflow testing, registering an in-mem event store and a delayed publisher.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
public class WorkflowTestEnhancer implements ConfigurationEnhancer {

    @Override
    public void enhance(@Nonnull ComponentRegistry registry) {
        registry
                .registerComponent(DelayedPublisher.class, cfg ->
                        new DelayedPublisher(
                                cfg.getComponent(EventSink.class),
                                cfg.getComponent(Executor.class, WORKFLOW_ENGINE_EXECUTOR),
                                cfg.getComponent(MessageTypeResolver.class),
                                cfg.getComponent(Converter.class)
                        )
                ).registerDecorator(EventStore.class,
                                    InterceptingEventStore.DECORATION_ORDER - 1,
                                    (configuration, name, delegate) ->
                                            PrettyPrintingRecordingEventStore.eventStore(delegate)
                );
    }
}
