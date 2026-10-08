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

package org.axonframework.deadline;

import org.axonframework.conversion.PassThroughConverter;
import org.axonframework.messaging.commandhandling.GenericCommandMessage;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.modelling.EntityIdResolutionException;
import org.axonframework.modelling.EntityIdResolver;
import org.axonframework.modelling.MetadataEntityIdResolver;
import org.axonframework.modelling.annotation.TargetEntityId;
import org.junit.jupiter.api.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test class validating the {@link AggregateDeadlineEntityIdResolverDefinition}.
 *
 * @author Steven van Beelen
 */
class AggregateDeadlineEntityIdResolverDefinitionTest {

    private AggregateDeadlineEntityIdResolverDefinition testSubject;

    @BeforeEach
    void setUp() {
        testSubject = new AggregateDeadlineEntityIdResolverDefinition();
    }

    @SuppressWarnings("DataFlowIssue")
    @Test
    void resolvesFromTheAggregateIdentifierMetadataKeyWhenThePayloadHasNoTargetEntityId() throws Exception {
        // given
        record Payload(String effect) {

        }
        EntityIdResolver<Object> resolver = testSubject.createIdResolver(Payload.class, Object.class, null, null);
        Message message = new GenericCommandMessage(
                new MessageType(Payload.class), new Payload("x"),
                Metadata.with(AggregateDeadlineCommandTranslator.DESCRIPTOR_BASED_ID, "entity-1")
        );

        // when
        Object result = resolver.resolve(message, StubProcessingContext.forMessage(message));

        // then
        assertThat(result).isEqualTo("entity-1");
    }

    @SuppressWarnings("DataFlowIssue")
    @Test
    void resolvesFromTargetEntityIdWhenPresentOnThePayload() throws Exception {
        // given
        record Payload(@TargetEntityId String id) {

        }
        EntityIdResolver<Object> resolver = testSubject.createIdResolver(Payload.class, Object.class, null, null);
        Message message = new GenericCommandMessage(new MessageType(Payload.class), new Payload("payload-id"));

        // when
        Object result = resolver.resolve(message, StubProcessingContext.forMessage(message));

        // then
        assertThat(result).isEqualTo("payload-id");
    }

    @SuppressWarnings("DataFlowIssue")
    @Test
    void rejectsANullMetadataEntityIdResolver() {
        assertThatThrownBy(() -> new AggregateDeadlineEntityIdResolverDefinition(null))
                .isInstanceOf(NullPointerException.class);
    }

    @SuppressWarnings("DataFlowIssue")
    @Test
    void usesTheGivenMetadataEntityIdResolverInstead() throws Exception {
        // given
        String customKey = "custom-entity-id";
        AggregateDeadlineEntityIdResolverDefinition testSubject = new AggregateDeadlineEntityIdResolverDefinition(
                new MetadataEntityIdResolver<>(customKey, String.class, PassThroughConverter.INSTANCE)
        );
        record Payload(String effect) {

        }
        EntityIdResolver<Object> resolver = testSubject.createIdResolver(Payload.class, Object.class, null, null);
        Message message = new GenericCommandMessage(
                new MessageType(Payload.class), new Payload("x"), Metadata.with(customKey, "entity-42")
        );

        // when
        Object result = resolver.resolve(message, StubProcessingContext.forMessage(message));

        // then
        assertThat(result).isEqualTo("entity-42");
    }

    @SuppressWarnings("DataFlowIssue")
    @Test
    void doesNotFallBackToTheDefaultAggregateIdentifierMetadataKey() {
        // given
        AggregateDeadlineEntityIdResolverDefinition testSubject = new AggregateDeadlineEntityIdResolverDefinition(
                new MetadataEntityIdResolver<>("custom-entity-id")
        );
        record Payload(String effect) {

        }
        EntityIdResolver<Object> resolver = testSubject.createIdResolver(Payload.class, Object.class, null, null);
        Message message = new GenericCommandMessage(
                new MessageType(Payload.class), new Payload("x"),
                Metadata.with(AggregateDeadlineCommandTranslator.DESCRIPTOR_BASED_ID, "entity-1")
        );

        // when / then
        assertThatThrownBy(() -> resolver.resolve(message, StubProcessingContext.forMessage(message)))
                .isInstanceOf(EntityIdResolutionException.class);
    }
}
