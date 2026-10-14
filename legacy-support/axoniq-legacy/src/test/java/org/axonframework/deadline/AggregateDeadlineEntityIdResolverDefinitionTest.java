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

import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.messaging.commandhandling.GenericCommandMessage;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.modelling.EntityIdResolutionException;
import org.axonframework.modelling.EntityIdResolver;
import org.axonframework.modelling.annotation.TargetEntityId;
import org.junit.jupiter.api.*;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.axonframework.deadline.AggregateDeadlineEntityIdResolverDefinition.DESCRIPTOR_BASED_ID;

/**
 * Test class validating the {@link AggregateDeadlineEntityIdResolverDefinition}.
 *
 * @author Steven van Beelen
 */
class AggregateDeadlineEntityIdResolverDefinitionTest {

    private AggregateDeadlineEntityIdResolverDefinition testSubject;
    private AxonConfiguration configuration;

    @BeforeEach
    void setUp() {
        testSubject = new AggregateDeadlineEntityIdResolverDefinition();
        configuration = MessagingConfigurer.create().start();
    }

    @AfterEach
    void tearDown() {
        configuration.shutdown();
    }

    @SuppressWarnings("DataFlowIssue") // We do not require the model for testing, so passed as null
    @Test
    void resolvesFromTargetEntityIdWhenPresentOnThePayload() throws Exception {
        // given
        record Payload(@TargetEntityId String id) {

        }
        EntityIdResolver<String> resolver =
                testSubject.createIdResolver(Payload.class, String.class, null, configuration);
        Message message = new GenericCommandMessage(
                new MessageType(Payload.class), new Payload("payload-id")
        );

        // when
        String result = resolver.resolve(message, StubProcessingContext.forMessage(message));

        // then
        assertThat(result).isEqualTo("payload-id");
    }

    @Nested
    class MetadataFallbackByIdentifierType {

        record Payload(String effect) {

        }

        @SuppressWarnings("DataFlowIssue") // We do not require the model for testing, so passed as null
        @Test
        void resolvesAStringIdentifierFromTheAggregateIdentifierMetadataKey() throws Exception {
            // given
            EntityIdResolver<String> resolver =
                    testSubject.createIdResolver(Payload.class, String.class, null, configuration);
            Message message = new GenericCommandMessage(
                    new MessageType(Payload.class), new Payload("x"),
                    Metadata.with(DESCRIPTOR_BASED_ID, "entity-1")
            );

            // when
            String result = resolver.resolve(message, StubProcessingContext.forMessage(message));

            // then
            assertThat(result).isEqualTo("entity-1");
        }

        @SuppressWarnings("DataFlowIssue") // We do not require the model for testing, so passed as null
        @Test
        void resolvesALongIdentifierFromTheAggregateIdentifierMetadataKey() throws Exception {
            // given
            Long entityId = 42L;
            EntityIdResolver<Long> resolver =
                    testSubject.createIdResolver(Payload.class, Long.class, null, configuration);
            Message message = new GenericCommandMessage(
                    new MessageType(Payload.class), new Payload("x"),
                    Metadata.with(DESCRIPTOR_BASED_ID, entityId.toString())
            );

            // when
            Long result = resolver.resolve(message, StubProcessingContext.forMessage(message));

            // then
            assertThat(result).isEqualTo(entityId);
        }

        @SuppressWarnings("DataFlowIssue") // We do not require the model for testing, so passed as null
        @Test
        void fallsBackToTheRawStringWhenTheDefaultConverterCannotConvertAUuidIdentifier() throws Exception {
            // given
            UUID entityId = UUID.randomUUID();
            EntityIdResolver<UUID> resolver =
                    testSubject.createIdResolver(Payload.class, UUID.class, null, configuration);
            Message message = new GenericCommandMessage(
                    new MessageType(Payload.class), new Payload("x"),
                    Metadata.with(DESCRIPTOR_BASED_ID, entityId.toString())
            );

            // when
            // The default Jackson-based GeneralConverter re-parses a String source as JSON, which requires a quoted
            // value. A raw UUID string isn't quoted, so conversion fails and the raw String is returned instead.
            Object result = resolver.resolve(message, StubProcessingContext.forMessage(message));

            // then
            assertThat(result).isEqualTo(entityId.toString());
        }

        @SuppressWarnings("DataFlowIssue") // We do not require the model for testing, so passed as null
        @Test
        void throwsEntityIdResolutionExceptionWhenTheMetadataEntryIsMissing() {
            // given
            EntityIdResolver<UUID> resolver =
                    testSubject.createIdResolver(Payload.class, UUID.class, null, configuration);
            Message message = new GenericCommandMessage(new MessageType(Payload.class), new Payload("x"));

            // when / then
            assertThatThrownBy(() -> resolver.resolve(message, StubProcessingContext.forMessage(message)))
                    .isInstanceOf(EntityIdResolutionException.class);
        }
    }
}
