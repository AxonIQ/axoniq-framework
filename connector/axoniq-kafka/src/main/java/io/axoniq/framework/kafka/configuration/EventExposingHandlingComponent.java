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

package io.axoniq.framework.kafka.configuration;

import io.axoniq.framework.kafka.publishing.RecordKeyResolver;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.Context.ResourceKey;
import org.axonframework.messaging.core.LegacyResources;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.eventhandling.DelegatingEventHandlingComponent;
import org.axonframework.messaging.eventhandling.EventSink;
import org.axonframework.messaging.eventhandling.SimpleEventHandlingComponent;

import java.util.Set;

/**
 * An event handling component that publishes the events it handles, as they are, through the given
 * {@link EventSink}.
 * <p>
 * It refuses resets: replaying would publish every event again, to consumers that already received them.
 * <p>
 * It marks the context it publishes in, so that the sink's {@link #keyingByAggregate(RecordKeyResolver) record key
 * resolver} can key exposed events by the identifier of the aggregate they belong to.
 * <p>
 * Internal, because {@link KafkaConnector#exposeEvents} builds it for the processor it configures.
 *
 * @author Allard Buijze
 * @since 5.5.0
 */
@Internal
final class EventExposingHandlingComponent extends DelegatingEventHandlingComponent {

    private static final ResourceKey<Boolean> EXPOSED = ResourceKey.withLabel("kafkaExposedEvent");

    /**
     * Constructs a component publishing the events with the given {@code eventNames} through the given {@code sink}.
     *
     * @param name       the name of the component
     * @param sink       the sink to publish through
     * @param eventNames the names of the events to publish
     */
    EventExposingHandlingComponent(String name, EventSink sink, Set<QualifiedName> eventNames) {
        super(SimpleEventHandlingComponent.create(name).subscribe(
                eventNames,
                (event, context) -> MessageStream.fromFuture(
                        sink.publish(context.withResource(EXPOSED, Boolean.TRUE), event)
                            .thenApply(ignored -> (Message) null)
                ).ignoreEntries()
        ));
    }

    /**
     * Returns a resolver that keys the events this component publishes by the identifier of the aggregate they belong
     * to, when the event store provides one, and resolves all other keys with the given {@code fallback}.
     * <p>
     * Records of the same aggregate then land in the same partition, so that consumers see them in order. Events that
     * a handler publishes explicitly, and exposed events without an aggregate, such as those of a tag-based event
     * store, are keyed by the {@code fallback}.
     *
     * @param fallback the resolver of the keys of all other events
     * @return a resolver keying exposed events by their aggregate identifier
     */
    static RecordKeyResolver keyingByAggregate(RecordKeyResolver fallback) {
        return (event, context) -> {
            if (context != null && context.containsResource(EXPOSED)) {
                String aggregateIdentifier = context.getResource(LegacyResources.AGGREGATE_IDENTIFIER_KEY);
                if (aggregateIdentifier != null) {
                    return aggregateIdentifier;
                }
            }
            return fallback.resolve(event, context);
        };
    }

    @Override
    public boolean supportsReset() {
        return false;
    }
}
