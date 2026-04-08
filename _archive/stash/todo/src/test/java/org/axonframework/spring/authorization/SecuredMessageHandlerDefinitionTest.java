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

package org.axonframework.spring.authorization;

import org.axonframework.extension.spring.authorization.SecuredMessageHandlerDefinition;
import org.axonframework.extension.spring.authorization.UnauthorizedMessageException;
import org.axonframework.test.aggregate.AggregateTestFixture;
import org.axonframework.test.aggregate.FixtureConfiguration;
import org.hamcrest.core.StringStartsWith;
import org.junit.jupiter.api.*;
import org.springframework.security.core.authority.SimpleGrantedAuthority;

import java.util.Map;
import java.util.UUID;

import static org.axonframework.test.matchers.Matchers.exactSequenceOf;
import static org.axonframework.test.matchers.Matchers.predicate;

class SecuredMessageHandlerDefinitionTest {

    private static final UUID TEST_AGGREGATE_IDENTIFIER = UUID.randomUUID();

    private FixtureConfiguration<TestAggregate> testSubject;

    @BeforeEach
    void setUp() {
        testSubject = new AggregateTestFixture<>(TestAggregate.class)
                .registerHandlerEnhancerDefinition(new SecuredMessageHandlerDefinition());
    }

    @Test
    @Disabled("TODO #3073 - Revisit Aggregate Test Fixture")
    void shouldAllowWhenAuthorityMatch() {
        Map<String, String> metadata = new java.util.HashMap<>();
        metadata.put("authorities", new SimpleGrantedAuthority("ROLE_aggregate.create").getAuthority());
        testSubject.givenNoPriorActivity()
                   .when(new CreateAggregateCommand(TEST_AGGREGATE_IDENTIFIER), metadata)
                   .expectEventsMatching(exactSequenceOf(predicate(
                           eventMessage -> eventMessage.payloadType().isAssignableFrom(AggregateCreatedEvent.class)
                   )));
    }

    @Test
    @Disabled("TODO #3073 - Revisit Aggregate Test Fixture")
    void shouldDenyWhenAuthorityDoesNotMatch() {
        Map<String, String> metadata = new java.util.HashMap<>();
        metadata.put("authorities", new SimpleGrantedAuthority("ROLE_anonymous").getAuthority());
        testSubject.givenNoPriorActivity()
                   .when(new CreateAggregateCommand(TEST_AGGREGATE_IDENTIFIER),
                         metadata)
                   .expectException(UnauthorizedMessageException.class)
                   .expectExceptionMessage(StringStartsWith.startsWith("Unauthorized message"));
    }

    @Test
    @Disabled("TODO #3073 - Revisit Aggregate Test Fixture")
    void shouldAllowUnannotatedMethods() {
        Map<String, String> metadata = new java.util.HashMap<>();
        metadata.put("authorities", new SimpleGrantedAuthority("ROLE_anonymous").getAuthority());
        testSubject.given(new AggregateCreatedEvent(TEST_AGGREGATE_IDENTIFIER))
                   .when(new UpdateAggregateCommand(TEST_AGGREGATE_IDENTIFIER), metadata)
                   .expectEventsMatching(exactSequenceOf(predicate(
                           eventMessage -> eventMessage.payloadType().isAssignableFrom(AggregateUpdatedEvent.class)
                   )));
    }

    @Test
    @Disabled("TODO #3073 - Revisit Aggregate Test Fixture")
    void shouldDenyWhenNoAuthorityPresent() {
        testSubject.givenNoPriorActivity()
                   .when(new CreateAggregateCommand(TEST_AGGREGATE_IDENTIFIER))
                   .expectException(UnauthorizedMessageException.class)
                   .expectExceptionMessage(StringStartsWith.startsWith("Unauthorized message"));
    }
}