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

import io.axoniq.framework.workflow.simulation.scenarios.ManagerQueryEquivalenceScenario;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Drives {@link ManagerQueryEquivalenceScenario}: for every named {@code WorkflowStateQuery} restriction the
 * {@code WorkflowManager} answers exactly the ids a fold of the committed log selects, each result satisfies
 * INVARIANTS.md INV-34 ({@code ManagerOneStatePerId}), and a detached state stays what it was when it was read.
 * The step-order probe is logged, not asserted: the detached state orders steps by timestamp, the history state and
 * the log by record order, and this test records whether they agree on this machine.
 *
 * @author Stefan Dragisic
 */
class ManagerQueryEquivalenceTest {

    private static final Logger logger = LoggerFactory.getLogger(ManagerQueryEquivalenceTest.class);

    @ParameterizedTest(name = "seed {0}: the manager agrees with the log fold on every named restriction")
    @ValueSource(longs = {0L, 11L, 42L})
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void everyNamedRestrictionAgreesWithTheLogFold(long seed) {
        var outcome = ManagerQueryEquivalenceScenario.run(seed);
        outcome.comparisons().forEach(c -> logger.info("seed {} probe {}: expected {} manager {} size {}",
                                                       seed, c.name(), c.expected(), c.managerIds(), c.managerSize()));
        logger.info("seed {} step order of A — detached {} / history {} / log {}", seed,
                    outcome.detachedStepOrderA(), outcome.historyStepOrderA(), outcome.logStepOrderA());

        assertThat(outcome.disagreements())
                .as("every probe's manager answer equals the log fold's selection")
                .isEmpty();
        assertThat(outcome.comparisons())
                .as("the probe set exercises every criterion kind, not only the empty ones")
                .anySatisfy(c -> assertThat(c.managerIds()).hasSize(3))
                .anySatisfy(c -> assertThat(c.managerIds()).hasSize(1))
                .anySatisfy(c -> assertThat(c.managerIds()).isEmpty());
        assertThat(outcome.detachedSnapshotUnchanged())
                .as("a detached state read while parked still reads STARTED with its one step after completion")
                .isTrue();
        assertThat(outcome.detachedStepOrderA())
                .as("the detached state of A holds both of its steps")
                .containsExactlyInAnyOrderElementsOf(outcome.logStepOrderA());
    }
}
