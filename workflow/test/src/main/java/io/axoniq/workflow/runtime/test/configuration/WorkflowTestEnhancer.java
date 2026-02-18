/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://lp.axoniq.io/axoniq-software-subscription-agreement-terms
 *
 *
 */
package io.axoniq.workflow.runtime.test.configuration;

import io.axoniq.workflow.runtime.test.utils.DelayedPublisher;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.eventsourcing.eventstore.InterceptingEventStore;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.eventhandling.EventSink;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.Executor;

import static io.axoniq.workflow.runtime.engine.configuration.WorkflowEnhancer.WORKFLOW_ENGINE_EXECUTOR;

public class WorkflowTestEnhancer implements ConfigurationEnhancer {

    @Override
    public void enhance(@NotNull ComponentRegistry registry) {
        registry
                .registerComponent(DelayedPublisher.class, cfg ->
                        new DelayedPublisher(
                                cfg.getComponent(EventSink.class),
                                cfg.getComponent(Executor.class, WORKFLOW_ENGINE_EXECUTOR),
                                cfg.getComponent(MessageTypeResolver.class)
                        )
                ).registerDecorator(EventStore.class,
                                    InterceptingEventStore.DECORATION_ORDER - 1,
                                    (configuration, name, delegate) ->
                                            PrettyPrintingRecordingEventStore.eventStore(delegate)
                );
    }
}
