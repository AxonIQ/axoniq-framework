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

package io.axoniq.framework.messaging.transformation.events;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that events stored without an explicit version are treated as version
 * {@code "0.0.1"} (AF5 default) and match transformations registered for that version.
 * No special API is required from the user.
 */
class EventTransformationFr016Test {

    private static final QualifiedName NAME = new QualifiedName("com.example.LegacyEvent");
    private static final MessageType DEFAULT_V = new MessageType(NAME, "0.0.1");
    private static final MessageType V2 = new MessageType(NAME, "2.0.0");

    @Test
    @Disabled("Tests-first; impl lands in T023 + T027")
    void unversioned_event_matches_a_transformation_registered_for_default_version() {
        // given -- transformer registered for the default version
        EventTransformer t = EventTransformation.from(DEFAULT_V).to(V2).transform(JsonNode.class, (in, ctx) -> in);
        EventTransformerChain chain = EventTransformerChain.builder().register(t).build();

        // and -- an event constructed with just QualifiedName (defaulting version to "0.0.1")
        EventMessage legacy = new GenericEventMessage(new MessageType(NAME), JsonNodeFactory.instance.objectNode());

        // sanity: the MessageType the framework picked is the default version
        assertThat(legacy.type().version()).isEqualTo("0.0.1");

        // when
        List<EventMessage> out = drain(chain.transform(MessageStream.fromIterable(List.of(legacy))));

        // then
        assertThat(out).hasSize(1);
        assertThat(out.get(0).type()).isEqualTo(V2);
    }

    private static List<EventMessage> drain(MessageStream<? extends EventMessage> stream) {
        return stream.<List<EventMessage>>reduce(new ArrayList<>(), (acc, entry) -> {
            acc.add(entry.message());
            return acc;
        }).join();
    }
}
