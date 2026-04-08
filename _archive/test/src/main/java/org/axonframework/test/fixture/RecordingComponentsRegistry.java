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
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */

package org.axonframework.test.fixture;

import org.axonframework.common.annotation.Internal;
import org.jspecify.annotations.Nullable;

/**
 * A registry that holds references to the recording components created by
 * {@link MessagesRecordingConfigurationEnhancer}. Decorators store recording instances into this registry, and the
 * {@link AxonTestFixture} reads them to perform assertions.
 * <p>
 * This registry is registered as a regular component in the {@link org.axonframework.common.configuration.Configuration}
 * and can be resolved via {@code configuration.getComponent(RecordingComponentsRegistry.class)}.
 *
 * @author Mateusz Nowak
 * @since 5.0.3
 */
@Internal
public class RecordingComponentsRegistry {

    private RecordingCommandBus commandBus;
    private RecordingEventSink eventSink;

    /**
     * Returns the {@link RecordingCommandBus} that captures command messages dispatched through the
     * {@link org.axonframework.messaging.commandhandling.CommandBus}.
     *
     * @return the recording command bus, or {@code null} if the decorator has not yet run
     */
    @Nullable
    public RecordingCommandBus commandBus() {
        return commandBus;
    }

    /**
     * Returns the {@link RecordingEventSink} that captures event messages published through the
     * {@link org.axonframework.messaging.eventhandling.EventBus} or
     * {@link org.axonframework.eventsourcing.eventstore.EventStore}.
     *
     * @return the recording event sink, or {@code null} if the decorator has not yet run
     */
    @Nullable
    public RecordingEventSink eventSink() {
        return eventSink;
    }

    void registerCommandBus(RecordingCommandBus commandBus) {
        this.commandBus = commandBus;
    }

    void registerEventSink(RecordingEventSink eventSink) {
        this.eventSink = eventSink;
    }
}
