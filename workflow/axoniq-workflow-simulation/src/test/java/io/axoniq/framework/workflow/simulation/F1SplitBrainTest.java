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

import io.axoniq.framework.workflow.simulation.invariants.Invariants;
import io.axoniq.framework.workflow.simulation.scenarios.SplitBrainScenario;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;

/**
 * Bridges TLA+ {@code MC_owner.cfg} / {@code AtMostOneOwner} (INVARIANTS.md INV-1, finding F-1), now closed by DCB
 * append conditions.
 * <p>
 * Two engine instances over one shared event store, each with its own non-durable processor token, both claim
 * segment 0 and both try to drive the same workflow instance. The store is the arbiter: only one writer's spawn append
 * is accepted, the other is rejected and interrupted, so exactly one owner routes the start. The former F-1
 * expectation of two owners was flipped to this assertion when the fencing landed.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
class F1SplitBrainTest {

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void twoEnginesOverSharedStore_onlyOneOwnerRoutesTheStart() {
        SplitBrainScenario.Outcome outcome = SplitBrainScenario.run(0L, "A");
        // F-1 closed: both engines claim segment 0, but the append condition lets exactly one spawn append land.
        Invariants.assertAtMostOneOwner(outcome.ownersThatRoutedStart());
    }
}
