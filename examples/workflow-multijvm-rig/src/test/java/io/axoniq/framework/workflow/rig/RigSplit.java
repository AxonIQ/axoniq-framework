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
package io.axoniq.framework.workflow.rig;

/**
 * The two halves this module's integration tests are run in.
 * <p>
 * The suite no longer fits in one invocation. Every scenario here waits out real claim timeouts, real retry backoffs
 * and real node boots, and an evaluation licence calls {@code System.exit} about fifteen minutes into a run - which
 * takes the forked node JVMs with it and leaves a truncated log that is void rather than green. Splitting the classes
 * over two runs keeps each one comfortably under that.
 * <p>
 * Every {@code *IT} class carries exactly one of these tags:
 * <pre>
 * ./mvnw -o -ntp verify -pl examples/multijvm-rig -am -Dgroups=rig-a -Dtest=none -Dsurefire.failIfNoSpecifiedTests=false
 * ./mvnw -o -ntp verify -pl examples/multijvm-rig -am -Dgroups=rig-b -Dtest=none -Dsurefire.failIfNoSpecifiedTests=false
 * </pre>
 * The halves are balanced by measured duration, not by count. A new class goes in whichever half is currently the
 * shorter one.
 */
final class RigSplit {

    /**
     * The half holding the scenarios that wait out timers and quiet periods.
     */
    static final String A = "rig-a";

    /**
     * The half holding the membership and failover scenarios.
     */
    static final String B = "rig-b";

    private RigSplit() {
    }
}
