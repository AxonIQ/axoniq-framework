/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.messaging.transformation.events;

import com.fasterxml.jackson.databind.JsonNode;
import org.axonframework.eventsourcing.eventstore.GlobalIndexPosition;
import org.axonframework.eventsourcing.eventstore.SnapshotEventMessage;
import org.axonframework.eventsourcing.snapshot.api.Snapshot;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.collectMessages;
import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.neverInvokedConverter;
import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.neverInvokedMessageTypeResolver;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * A snapshot in a sourced stream carries entity state rather than an event, so the chain passes it through untouched,
 * even when a transformation matches every event.
 *
 * @author John Hendrikx
 */
final class EventTransformerChainSnapshotPassThroughTest {

    @Test
    void shouldPassASnapshotThroughEvenWhenATransformationMatchesEveryEvent() {
        // given
        EventTransformation matchingEverything = EventTransformation.from(type -> true)
                                                                    .to(new MessageType("com.example.Anything", "2"))
                                                                    .transform(JsonNode.class, (in, ctx) -> in);
        EventTransformerChain chain = EventTransformerChain.builder().register(matchingEverything).build();
        SnapshotEventMessage snapshot = new SnapshotEventMessage(
                new QualifiedName("account"),
                new Snapshot(new GlobalIndexPosition(1), "1.0.0", "state", Instant.EPOCH, Map.of())
        );

        // when
        List<EventMessage> observed = collectMessages(chain.transform(
                MessageStream.fromIterable(List.of(snapshot)), null, neverInvokedConverter(),
                neverInvokedMessageTypeResolver()
        ));

        // then
        assertThat(observed).containsExactly(snapshot);
    }
}
