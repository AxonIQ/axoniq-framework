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
package io.axoniq.framework.workflow.simulation;

import io.axoniq.framework.workflow.simulation.scenarios.FencedWriterParkScenario;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Expected-gap pin for the liveness ADR-015 trades away: "Two interleaved writers can both get rejected and both stop;
 * the instance stays durable and parks until the next claim restores it. Safety over liveness."
 * <p>
 * The assertion is that the gap is PRESENT: a single late foreign write parks a healthy instance on a node that never
 * moves a claim, and only a claim un-parks it. The day the engine recovers a fenced instance by itself, this test
 * flips and the trade-off has changed.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
class FencedWriterParkTest {

    private static final Logger logger = LoggerFactory.getLogger(FencedWriterParkTest.class);

    @Test
    @Timeout(value = 3, unit = TimeUnit.MINUTES)
    void aFencedInstanceParksUntilTheNextClaim() {
        var outcome = FencedWriterParkScenario.run(7L, "parked-by-fence");
        logger.info("Fenced writer park: {}", outcome);

        assertThat(outcome.fencedInstance())
                .as("the fence must have written for the instance, or this run proves nothing")
                .isEqualTo("order-parked-by-fence");
        assertThat(outcome.terminalWhileParked())
                .as("no claim moved, so the fenced instance must still be non-terminal after %s of simulated time",
                    outcome.simulatedParkTime())
                .isFalse();
        assertThat(outcome.terminalAfterClaim())
                .as("the next claim restores the instance and it finishes")
                .isTrue();
        assertThat(outcome.workflowTerminalRecords())
                .as("the park never costs a duplicate terminal record")
                .isLessThanOrEqualTo(1);
    }
}
