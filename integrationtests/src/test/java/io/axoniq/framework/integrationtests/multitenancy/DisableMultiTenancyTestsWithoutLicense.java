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

package io.axoniq.framework.integrationtests.multitenancy;

import io.axoniq.framework.integrationtests.testsuite.infrastructure.AxonServerTestInfrastructure;
import org.junit.jupiter.api.extension.*;

// FIXME: this is a temparary solution to disable multi-tenancy tests when no Axon Server license is present.
//  Once the license file can be resolved in github ci runs, this can be removed.
public class DisableMultiTenancyTestsWithoutLicense implements ExecutionCondition {

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
        return AxonServerTestInfrastructure.licenseExists()
                ? ConditionEvaluationResult.enabled("Axon Server license exists, multi-tenancy tests can run")
                : ConditionEvaluationResult.disabled("Axon Server license does not exist, multi-tenancy tests are disabled");
    }
}
