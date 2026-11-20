/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import org.axonframework.conversion.Converter;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.conversion.jackson2.Jackson2Converter;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that a recorded hibernated workflow counts as still hibernated only while its open waits are exactly the ones
 * it hibernated with, and that a record survives the {@link Converter} implementations that store it.
 *
 * @author Stefan Dragisic
 */
class HibernatedWorkflowsTest {

    private static final Instant WAKES_AT = Instant.parse("2026-11-20T10:00:00Z");
    private static final HibernatedWorkflows.HibernatedWorkflow WAITING_FOR_PAYMENT_AND_SHIPMENT =
            new HibernatedWorkflows.HibernatedWorkflow(Set.of("awaitPayment", "awaitShipment"), WAKES_AT.toString());
    private final HibernatedWorkflows recorded =
            new HibernatedWorkflows(Map.of("order-1", WAITING_FOR_PAYMENT_AND_SHIPMENT));

    static Stream<Arguments> converters() {
        return Stream.of(Arguments.of(Named.of("JacksonConverter", new JacksonConverter())),
                         Arguments.of(Named.of("Jackson2Converter", new Jackson2Converter())));
    }

    @Nested
    class Hibernated {

        @Test
        void returnsTheWorkflowWhenItsOpenWaitsAreUnchanged() {
            // when
            var result = recorded.hibernated("order-1", Set.of("awaitShipment", "awaitPayment"));

            // then
            assertThat(result).contains(WAITING_FOR_PAYMENT_AND_SHIPMENT);
        }

        @Test
        void returnsEmptyWhenAWaitClosedSinceItHibernated() {
            // when: the payment arrived, which closed its wait
            var result = recorded.hibernated("order-1", Set.of("awaitShipment"));

            // then
            assertThat(result).isEmpty();
        }

        @Test
        void returnsEmptyForAWorkflowWithoutRecord() {
            // when
            var result = recorded.hibernated("order-2", Set.of("awaitPayment", "awaitShipment"));

            // then
            assertThat(result).isEmpty();
        }
    }

    @Nested
    class Conversion {

        @ParameterizedTest
        @MethodSource("io.axoniq.framework.workflow.runtime.execution.HibernatedWorkflowsTest#converters")
        void readsBackTheRecordItWrote(Converter converter) {
            // when
            byte[] written = converter.convert(recorded, byte[].class);
            var read = converter.convert(written, HibernatedWorkflows.class);

            // then
            assertThat(read).isEqualTo(recorded);
        }
    }
}
