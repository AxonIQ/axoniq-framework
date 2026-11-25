/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import org.junit.jupiter.api.*;

import java.time.Instant;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that a hibernated workflow counts as still hibernated only while its open waits are exactly the ones it
 * hibernated with.
 *
 * @author Stefan Dragisic
 */
class HibernatedWorkflowsTest {

    private static final Instant WAKES_AT = Instant.parse("2026-11-20T10:00:00Z");
    private static final HibernatedWorkflows.HibernatedWorkflow WAITING_FOR_PAYMENT_AND_SHIPMENT =
            new HibernatedWorkflows.HibernatedWorkflow(Set.of("awaitPayment", "awaitShipment"), WAKES_AT);
    private final HibernatedWorkflows hibernated =
            new HibernatedWorkflows(Map.of("order-1", WAITING_FOR_PAYMENT_AND_SHIPMENT));

    @Nested
    class Hibernated {

        @Test
        void returnsTheWorkflowWhenItsOpenWaitsAreUnchanged() {
            // when
            var result = hibernated.hibernated("order-1", Set.of("awaitShipment", "awaitPayment"));

            // then
            assertThat(result).contains(WAITING_FOR_PAYMENT_AND_SHIPMENT);
        }

        @Test
        void returnsEmptyWhenAWaitClosedSinceItHibernated() {
            // when: the payment arrived, which closed its wait
            var result = hibernated.hibernated("order-1", Set.of("awaitShipment"));

            // then
            assertThat(result).isEmpty();
        }

        @Test
        void returnsEmptyForAWorkflowThatIsNotHibernated() {
            // when
            var result = hibernated.hibernated("order-2", Set.of("awaitPayment", "awaitShipment"));

            // then
            assertThat(result).isEmpty();
        }
    }
}
