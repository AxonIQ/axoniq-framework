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
package io.axoniq.workflow.runtime.test.utils;

import io.axoniq.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.workflow.runtime.util.MetadataUtils;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.common.infra.DescribableComponent;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.junit.jupiter.api.*;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Tests for {@link PrettyPrintingRecordingEventStore}.
 *
 * @author Simon Zambrovski
 */
class PrettyPrintingRecordingEventStoreTest {

    @Test
    void eventStoreWrapsDelegateOnlyOnceAndTracksLastInstance() {
        EventStore delegate = mock(EventStore.class);

        EventStore wrapped = PrettyPrintingRecordingEventStore.eventStore(delegate);
        EventStore wrappedAgain = PrettyPrintingRecordingEventStore.eventStore(wrapped);

        assertThat(wrapped).isInstanceOf(PrettyPrintingRecordingEventStore.class);
        assertThat(wrappedAgain).isSameAs(wrapped);
        assertThat(PrettyPrintingRecordingEventStore.lastInstance()).isSameAs(wrapped);
    }

    @Test
    void describeToGroupsRecordedWorkflowEvents() {
        EventStore delegate = mock(EventStore.class);
        when(delegate.publish(isNull(), anyList())).thenReturn(CompletableFuture.completedFuture(null));
        PrettyPrintingRecordingEventStore eventStore = new PrettyPrintingRecordingEventStore(delegate);
        ComponentDescriptor descriptor = mock(ComponentDescriptor.class);
        AtomicReference<Collection<?>> workflowEvents = new AtomicReference<>();
        doAnswer(invocation -> {
            workflowEvents.set(invocation.getArgument(1));
            return null;
        }).when(descriptor).describeProperty(eq("workflowEvents"), any(Collection.class));

        eventStore.publish(null,
                           List.of(new GenericEventMessage(new MessageType("StepCompleted"),
                                                           Map.of("result", "ok"),
                                                           MetadataUtils.create("wf-1", "charge", StepStatus.COMPLETED)
                                                                        .and("correlationId", "ignored")),
                                   new GenericEventMessage(new MessageType("Ignored"),
                                                           Map.of(),
                                                           Metadata.with("other", "value"))))
                  .join();
        eventStore.describeTo(descriptor);

        assertThat(workflowEvents.get()).hasSize(1);

        DescribableComponent workflowEvent = (DescribableComponent) workflowEvents.get().iterator().next();
        ComponentDescriptor nestedDescriptor = mock(ComponentDescriptor.class);
        AtomicReference<Collection<?>> describedEvents = new AtomicReference<>();
        doAnswer(invocation -> {
            describedEvents.set(invocation.getArgument(1));
            return null;
        }).when(nestedDescriptor).describeProperty(eq("wf-1"), any(Collection.class));

        workflowEvent.describeTo(nestedDescriptor);

        assertThat(describedEvents.get())
                .singleElement()
                .asString()
                .contains("StepCompleted")
                .contains("COMPLETED")
                .contains("charge")
                .doesNotContain("correlationId");
    }
}
