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

package io.axoniq.framework.tracing.integration;

import org.axonframework.messaging.tracing.Span;
import org.axonframework.messaging.tracing.SpanFactory;
import org.axonframework.messaging.tracing.attributes.AggregateIdentifierSpanAttributesProvider;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.data.SpanData;
import org.axonframework.messaging.core.LegacyResources;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the end-to-end behaviour of {@link AggregateIdentifierSpanAttributesProvider}: the
 * {@code axoniq.aggregate.identifier} span attribute is present when {@link LegacyResources#AGGREGATE_IDENTIFIER_KEY}
 * is populated on the {@link org.axonframework.messaging.core.unitofwork.ProcessingContext} (legacy aggregate-based
 * paths), and absent when it is not (DCB / entity-based paths). Uses the real Micrometer {@code SpanFactory}
 * (assembled by {@link MicrometerTracingTestSetup}) + {@link InMemorySpanExporter} so the attribute is asserted on the
 * exported {@link SpanData}.
 */
class AggregateIdentifierAttributeIntegrationTest {

    private MicrometerTracingTestSetup tracing;
    private InMemorySpanExporter spanExporter;
    private SpanFactory spanFactory;

    @BeforeEach
    void setUp() {
        tracing = MicrometerTracingTestSetup.create();
        spanExporter = tracing.spanExporter();
        spanFactory = tracing.spanFactory(List.of(new AggregateIdentifierSpanAttributesProvider()));
    }

    @AfterEach
    void tearDown() {
        tracing.close();
    }

    @Test
    void aggregateIdentifierIsAddedWhenTheLegacyResourceIsPopulated() {
        // given a context with LegacyResources.AGGREGATE_IDENTIFIER_KEY populated (legacy aggregate-based path)
        StubProcessingContext context = new StubProcessingContext();
        context.putResource(LegacyResources.AGGREGATE_IDENTIFIER_KEY, "room-42");
        EventMessage event = new GenericEventMessage(new MessageType("RoomBooked"), "payload");

        // when a span is opened for the event with that context
        Span span = spanFactory.createHandlerSpan("EventProcessor.process RoomBooked", event, context);
        span.start().close();

        // then the exported span carries the axoniq.aggregate.identifier attribute
        SpanData exported = onlySpan();
        assertThat(exported.getAttributes().get(AttributeKey.stringKey("axoniq.aggregate.identifier")))
                .as("axoniq.aggregate.identifier on the span")
                .isEqualTo("room-42");
    }

    @Test
    void aggregateIdentifierIsAbsentOnDcbOrEntityPaths() {
        // given a context WITHOUT LegacyResources.AGGREGATE_IDENTIFIER_KEY (DCB / entity-based path)
        StubProcessingContext context = new StubProcessingContext();
        EventMessage event = new GenericEventMessage(new MessageType("RoomBooked"), "payload");

        // when
        Span span = spanFactory.createHandlerSpan("EventProcessor.process RoomBooked", event, context);
        span.start().close();

        // then the span does not carry the aggregate-identifier attribute (provider returns an empty map)
        SpanData exported = onlySpan();
        assertThat(exported.getAttributes().get(AttributeKey.stringKey("axoniq.aggregate.identifier")))
                .as("axoniq.aggregate.identifier on a DCB-path span")
                .isNull();
    }

    private SpanData onlySpan() {
        assertThat(spanExporter.getFinishedSpanItems()).hasSize(1);
        return spanExporter.getFinishedSpanItems().get(0);
    }
}
