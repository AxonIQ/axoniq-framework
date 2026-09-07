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

import io.axoniq.framework.workflow.simulation.scenarios.InterruptedTerminalTransitionScenario;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Expected-gap pin: an interrupt inside a terminal transition makes one execution record the same terminal fact twice,
 * and the append condition does not prevent it.
 * <p>
 * The pin asserts the gap is PRESENT. It flips the day a terminal transition either survives the interrupt or refuses
 * to publish a second terminal event for a fact it already published. See
 * {@link InterruptedTerminalTransitionScenario} for the window and why the condition cannot see it.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
class InterruptedTerminalTransitionTest {

    private static final Logger logger = LoggerFactory.getLogger(InterruptedTerminalTransitionTest.class);

    @Test
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    void anInterruptedTerminalTransitionRecordsItsTerminalFactTwice() {
        var outcome = InterruptedTerminalTransitionScenario.run(5L, "dbl", 6);
        logger.info("Interrupted terminal transition: {}", outcome);

        assertThat(outcome.caughtWindow())
                .as("no attempt interrupted the driver inside the window, so this run proves nothing")
                .isTrue();
        assertThat(outcome.terminalRecords())
                .as("the interrupted transition republishes, and the condition accepts it: the same writer, the marker "
                            + "its own first append advanced")
                .isEqualTo(2);
        assertThat(outcome.distinctTerminalTypes())
                .as("both records are the same terminal fact, not two different outcomes")
                .isEqualTo(1);
    }
}
