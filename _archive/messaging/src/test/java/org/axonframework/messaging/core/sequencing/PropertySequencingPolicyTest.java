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

package org.axonframework.messaging.core.sequencing;

import org.jspecify.annotations.NonNull;
import org.axonframework.conversion.ConversionException;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventTestUtils;
import org.axonframework.messaging.eventhandling.conversion.DelegatingEventConverter;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.junit.jupiter.api.*;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class validating the {@link PropertySequencingPolicy}.
 *
 * @author Nils Christian Ehmke
 */
final class PropertySequencingPolicyTest {

    @Test
    void propertyExtractorShouldReadCorrectValue() {
        final SequencingPolicy<Message> sequencingPolicy =
                new ExtractionSequencingPolicy<>(
                        TestEvent.class,
                        TestEvent::id
                );

        assertThat(sequencingPolicy.sequenceIdentifierFor(
                anEvent(new TestEvent("42")),
                aProcessingContext())
        ).hasValue("42");
    }

    @Test
    void propertyShouldReadCorrectValue() {
        final SequencingPolicy<Message> sequencingPolicy = new PropertySequencingPolicy<>(
                TestEvent.class,
                "id"
        );

        assertThat(sequencingPolicy.sequenceIdentifierFor(anEvent(new TestEvent("42")), aProcessingContext())
        ).hasValue("42");
    }

    @Test
    void withoutFallbackShouldThrowException() {
        final SequencingPolicy<Message> sequencingPolicy = new PropertySequencingPolicy<>(
                TestEvent.class,
                "id"
        );
        EventMessage exMessage = anEvent("42");
        StubProcessingContext exContext = aProcessingContext();

        assertThrows(ConversionException.class,
                     () -> sequencingPolicy.sequenceIdentifierFor(exMessage, exContext));
    }

    @Test
    void withFallbackShouldNotThrowException() {
        final SequencingPolicy<Message> sequencingPolicy = new FallbackSequencingPolicy<>(
                new PropertySequencingPolicy<>(
                        TestEvent.class,
                        "id"
                ),
                (event, context) -> Optional.of("A"),
                ConversionException.class
        );

        assertThat(sequencingPolicy.sequenceIdentifierFor(
                anEvent("42"),
                aProcessingContext())
        ).hasValue("A");
    }

    private EventMessage anEvent(final Object payload) {
        return EventTestUtils.asEventMessage(payload);
    }

    private static StubProcessingContext aProcessingContext() {
        return StubProcessingContext.withComponent(EventConverter.class, eventConverter());
    }

    static @NonNull EventConverter eventConverter() {
        return new DelegatingEventConverter(new JacksonConverter());
    }

    private record TestEvent(String id) {

    }
}
