/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.conversion.DelegatingEventConverter;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Reproducer for the routing cost of the wait association index: {@link WaitAssociationIndex#match} checks every open
 * wait for every event that carries no workflow id, on the processor's single coordinator thread. Prints the time of
 * one match for growing numbers of open waits, for an event type no workflow waits for (a start event) and for one that
 * every open wait expects (a payment, which converts the payload once per row).
 *
 * @author Stefan Dragisic
 */
@Tag("perf")
class WaitAssociationIndexMatchCostTest {

    private static final Logger logger = LoggerFactory.getLogger(WaitAssociationIndexMatchCostTest.class);
    private static final QualifiedName PAYMENT_RECEIVED = new QualifiedName("PaymentReceived");
    private static final DelegatingEventConverter CONVERTER = new DelegatingEventConverter(new JacksonConverter());

    @Test
    void matchTimeGrowsWithTheNumberOfOpenWaits() {
        for (int waits : new int[]{1_000, 10_000, 100_000}) {
            var index = new WaitAssociationIndex(id -> true);
            index.segmentLoaded(Segment.ROOT_SEGMENT, IntStream.range(0, waits).mapToObj(
                    i -> new WaitAssociation("order-" + i, "awaitPayment", PAYMENT_RECEIVED,
                                             Set.of("payload:orderId=ord-" + i))).toList());
            var unrelated = event(new QualifiedName("OrderPlaced"), "{\"orderId\":\"ord-new\"}");
            var payment = event(PAYMENT_RECEIVED, "{\"orderId\":\"ord-7\",\"amount\":149.90}");

            double unrelatedMs = millisPerMatch(index, unrelated, waits);
            double paymentMs = millisPerMatch(index, payment, waits);
            double completionMs = millisPerCompletion(index, waits);

            assertThat(index.match(payment, new StubProcessingContext())).containsExactly("order-7");
            logger.info(String.format("PERF-MATCH open_waits=%d unrelated_event_ms=%.3f awaited_event_ms=%.3f "
                                              + "max_awaited_events_per_s=%.0f workflow_completed_evolve_ms=%.3f",
                                      waits, unrelatedMs, paymentMs, 1000 / paymentMs, completionMs));
        }
    }

    private static EventMessage event(QualifiedName name, String json) {
        // Events read from a store carry serialized payloads; the index converts them per row.
        return new GenericEventMessage(new MessageType(name), json.getBytes(StandardCharsets.UTF_8))
                .withConverter(CONVERTER);
    }

    /**
     * A workflow that ends removes its rows with a scan over every row, which every completion event pays.
     */
    private static double millisPerCompletion(WaitAssociationIndex index, int waits) {
        int rounds = Math.max(30, 2_000_000 / waits);
        long start = System.nanoTime();
        for (int i = 0; i < rounds; i++) {
            index.evolve(new GenericEventMessage(new MessageType(new QualifiedName("WorkflowCompleted")),
                                                 Map.of(),
                                                 MetadataUtils.create("done-" + i, WorkflowStatus.COMPLETED)));
        }
        return (System.nanoTime() - start) / 1_000_000.0 / rounds;
    }

    private static double millisPerMatch(WaitAssociationIndex index, EventMessage event, int waits) {
        var context = new StubProcessingContext();
        int rounds = Math.max(30, 2_000_000 / waits);
        for (int i = 0; i < rounds; i++) {
            index.match(event, context);
        }
        long start = System.nanoTime();
        for (int i = 0; i < rounds; i++) {
            index.match(event, context);
        }
        return (System.nanoTime() - start) / 1_000_000.0 / rounds;
    }
}
