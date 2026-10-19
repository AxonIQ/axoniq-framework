/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.test.utils;

import io.axoniq.framework.workflow.dsl.api.StepStatus;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
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
