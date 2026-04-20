/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */

package io.axoniq.framework.integrationtests.testsuite.student;

import io.axoniq.framework.integrationtests.testsuite.infrastructure.AxonServerTestInfrastructure;
import org.axonframework.integrationtests.testsuite.infrastructure.TestInfrastructure;
import org.axonframework.integrationtests.testsuite.student.ReplayStatusChangedHandlerIT;

/**
 * Runs {@link org.axonframework.integrationtests.testsuite.student.ReplayStatusChangedHandlerIT} against a real Axon
 * Server instance.
 */
public class ReplayStatusChangedHandlerAxonServerIT extends ReplayStatusChangedHandlerIT {

    private static final TestInfrastructure INFRASTRUCTURE = new AxonServerTestInfrastructure();

    @Override
    protected TestInfrastructure testInfrastructure() {
        return INFRASTRUCTURE;
    }
}
