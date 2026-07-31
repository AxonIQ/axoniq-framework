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

package io.axoniq.framework.integrationtests.testsuite.administration;

import io.axoniq.framework.integrationtests.testsuite.infrastructure.AxonServerTestInfrastructure;
import org.axonframework.integrationtests.testsuite.administration.ImmutableBuilderEntityModelAdministrationIT;
import org.axonframework.integrationtests.testsuite.infrastructure.TestInfrastructure;

/**
 * Runs {@link org.axonframework.integrationtests.testsuite.administration.ImmutableBuilderEntityModelAdministrationIT}
 * against a real Axon Server instance.
 */
public class ImmutableBuilderEntityModelAdministrationAxonServerIT extends ImmutableBuilderEntityModelAdministrationIT {

    private static final TestInfrastructure INFRASTRUCTURE = AxonServerTestInfrastructure.singleTenant();

    @Override
    protected TestInfrastructure testInfrastructure() {
        return INFRASTRUCTURE;
    }
}
