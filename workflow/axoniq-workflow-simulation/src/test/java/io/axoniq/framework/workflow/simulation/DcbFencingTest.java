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

import io.axoniq.framework.workflow.simulation.scenarios.DcbFencingScenario;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * The fenced counterpart of {@link F1SplitBrainTest} / {@link F1RecordDuplicationTest}: with DCB append conditions
 * active, the two-engine split-brain race over one shared store must leave a single-writer-clean committed log.
 * <p>
 * Per seed, safety must hold unconditionally: at most 1 workflow-status STARTED record, at most 1 terminal record per
 * {@code (workflowId, stepName)}, at most 1 workflow-terminal record. A seed only COUNTS as evidence when the fault
 * provably landed — at least one append rejection was observed for the instance
 * ({@code WorkflowAppendConditions} warning); a seed without an observed rejection is INCONCLUSIVE and another seed is
 * drawn. The run needs at least {@value #REQUIRED_CONCLUSIVE_SEEDS} conclusive seeds.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
class DcbFencingTest {

    private static final Logger logger = LoggerFactory.getLogger(DcbFencingTest.class);

    private static final int REQUIRED_CONCLUSIVE_SEEDS = 20;
    private static final int MAX_SEED_ATTEMPTS = 40;

    @Test
    @Timeout(value = 780, unit = TimeUnit.SECONDS)
    void twoEnginesOverSharedStore_fencingHolds_overSeeds() {
        int conclusive = 0;
        List<Long> inconclusiveSeeds = new ArrayList<>();
        List<String> violations = new ArrayList<>();

        for (long seed = 0; seed < MAX_SEED_ATTEMPTS && conclusive < REQUIRED_CONCLUSIVE_SEEDS; seed++) {
            DcbFencingScenario.Outcome outcome = DcbFencingScenario.run(seed, "F" + seed);
            logger.info("DcbFencing seed={} rejections={} started={} maxStepTerminal={} wfTerminal={} liveOwners={}",
                        seed, outcome.rejectionsObserved(), outcome.startedRecords(),
                        outcome.maxTerminalRecordsForAStep(), outcome.workflowTerminalRecords(),
                        outcome.liveOwners());

            // Safety holds regardless of whether the race fired on this seed.
            if (outcome.startedRecords() > 1) {
                violations.add("seed " + seed + ": " + outcome.startedRecords() + " STARTED records (expected ≤1)");
            }
            if (outcome.maxTerminalRecordsForAStep() > 1) {
                violations.add("seed " + seed + ": " + outcome.maxTerminalRecordsForAStep()
                                       + " terminal records for one step (expected ≤1)");
            }
            if (outcome.workflowTerminalRecords() > 1) {
                violations.add("seed " + seed + ": " + outcome.workflowTerminalRecords()
                                       + " workflow-terminal records (expected ≤1)");
            }

            // Evidence the fault landed: only a seed with an observed rejection counts toward the verdict.
            if (outcome.rejectionsObserved() >= 1) {
                conclusive++;
            } else {
                inconclusiveSeeds.add(seed);
            }
        }

        if (!violations.isEmpty()) {
            fail("DCB fencing violated:\n" + String.join("\n", violations));
        }
        logger.info("DcbFencing verdict: {} conclusive seeds (needed {}), inconclusive seeds: {}",
                    conclusive, REQUIRED_CONCLUSIVE_SEEDS, inconclusiveSeeds);
        assertThat(conclusive >= REQUIRED_CONCLUSIVE_SEEDS)
                .as("only " + conclusive + " of the required " + REQUIRED_CONCLUSIVE_SEEDS
                           + " seeds observed an append rejection (fault-landed proof); inconclusive seeds: "
                           + inconclusiveSeeds)
                .isTrue();
    }
}
