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

import io.axoniq.framework.workflow.simulation.scenarios.FencedVersionMigrationScenario;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A migrated branch running on a node that lost the instance records nothing, and the marker stays single. See
 * {@link FencedVersionMigrationScenario}.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
class FencedVersionMigrationTest {

    private static final Logger logger = LoggerFactory.getLogger(FencedVersionMigrationTest.class);

    @Test
    @Timeout(value = 3, unit = TimeUnit.MINUTES)
    void aFencedMigratedBranchRecordsNothingAndLeavesOneMarker() {
        var outcome = FencedVersionMigrationScenario.run(13L, "r3");
        logger.info("Fenced version migration: {}", outcome);

        assertThat(outcome.rejections())
                .as("the migrated branch's append must have been rejected, or this run proves nothing")
                .isGreaterThanOrEqualTo(1);
        assertThat(outcome.recordsAfterFence())
                .as("nothing more is accepted for the instance after the foreign write")
                .isEqualTo(outcome.recordsBeforeFence());
        assertThat(outcome.migrationMarkers())
                .as("the instance holds exactly one migration marker, so its routing stays unambiguous")
                .isEqualTo(1);
        assertThat(outcome.terminalRecords())
                .as("a fenced execution publishes nothing terminal")
                .isZero();
    }
}
