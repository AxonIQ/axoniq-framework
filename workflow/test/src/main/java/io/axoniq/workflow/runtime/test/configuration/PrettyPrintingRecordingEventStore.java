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
package io.axoniq.workflow.runtime.test.configuration;

import io.axoniq.workflow.runtime.util.MetadataUtils;
import jakarta.annotation.Nonnull;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.common.infra.DescribableComponent;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.test.fixture.RecordingEventStore;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static io.axoniq.workflow.runtime.util.MetadataUtils.METADATA_KEY_WORKFLOW_ID;

/**
 * Pretty printing event store used for testing.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
public class PrettyPrintingRecordingEventStore extends RecordingEventStore {

    private static final Set<String> IGNORED_KEYS = Set.of("correlationId", "causationId");
    private static volatile PrettyPrintingRecordingEventStore lastInstance;

    /**
     * Constructs a new {@link PrettyPrintingRecordingEventStore} wrapping the given {@link EventStore}.
     *
     * @param delegate event store to wrap.
     */
    public PrettyPrintingRecordingEventStore(@Nonnull EventStore delegate) {
        super(delegate);
        lastInstance = this;
    }

    @Nonnull
    public static PrettyPrintingRecordingEventStore lastInstance() {
        return Objects.requireNonNull(lastInstance, "No PrettyPrintingRecordingEventStore has been created yet");
    }

    @Nonnull
    public static EventStore eventStore(@Nonnull EventStore delegate) {
        if (delegate instanceof PrettyPrintingRecordingEventStore) {
            return delegate;
        } else {
            return new PrettyPrintingRecordingEventStore(delegate);
        }
    }

    @Override
    public void describeTo(@Nonnull ComponentDescriptor descriptor) {
        var eventsByWorkflowId = recorded().stream()
                                           .filter(e -> e.metadata().containsKey(METADATA_KEY_WORKFLOW_ID))
                                           .collect(Collectors.groupingBy(e -> e.metadata().getOrDefault(
                                                   METADATA_KEY_WORKFLOW_ID,
                                                   "none")));
        var events = eventsByWorkflowId.entrySet().stream()
                                       .filter(entry -> !"none".equals(entry.getKey()))
                                       .map(e -> new WorkflowEventDescriptor(e.getKey(), e.getValue()))
                                       .toList();
        descriptor.describeProperty("workflowEvents", events);
    }

    record WorkflowEventDescriptor(
            String workflowId,
            List<EventMessage> events
    ) implements DescribableComponent {

        @Override
        public void describeTo(@Nonnull ComponentDescriptor descriptor) {
            descriptor.describeProperty(workflowId, events.stream().map(event -> {
                var status = MetadataUtils.getStepStatus(event.metadata()).map(Enum::name)
                                          .or(() -> MetadataUtils.getWorkflowStatus(event.metadata()).map(Enum::name))
                                          .orElse("none");
                var name = event.type().qualifiedName().toString();
                return String.format("%s (%s): %s, %s", name, status, event.payload(),
                                     event.metadata().withoutKeys(IGNORED_KEYS));
            }).toList());
        }
    }
}
