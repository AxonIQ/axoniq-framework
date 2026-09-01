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

import io.axoniq.workflow.runtime.test.utils.PrettyPrintingRecordingEventStore;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.eventsourcing.eventstore.InterceptingEventStore;

/**
 * Test configuration enhancer that decorates the event store with a pretty-printing recording wrapper.
 *
 * <p>This enhancer installs {@link PrettyPrintingRecordingEventStore} as a decorator around the configured
 * {@link EventStore}. The wrapper records published events during a test and exposes them in a workflow-oriented,
 * human-readable form, which is useful when inspecting test failures, component descriptions, or recorded workflow
 * progress.</p>
 *
 * <p>The decorator is registered just ahead of Axon's {@link InterceptingEventStore}, so tests still interact with the
 * normal event-store pipeline while gaining access to the recorded and formatted event view provided by
 * {@link PrettyPrintingRecordingEventStore}.</p>
 *
 * <p>Use this enhancer when a workflow test benefits from inspecting emitted events in addition to asserting workflow
 * state or history, especially when diagnosing multi-step progress or comparing expected workflow event sequences.</p>
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
public class WorkflowTestPrettyRecordingEventStoreEnhancer implements ConfigurationEnhancer {

    @Override
    public void enhance(ComponentRegistry registry) {
        registry.registerDecorator(EventStore.class,
                                   InterceptingEventStore.DECORATION_ORDER - 1,
                                   (configuration, name, delegate) ->
                                           PrettyPrintingRecordingEventStore.eventStore(delegate)
        );
    }
}
