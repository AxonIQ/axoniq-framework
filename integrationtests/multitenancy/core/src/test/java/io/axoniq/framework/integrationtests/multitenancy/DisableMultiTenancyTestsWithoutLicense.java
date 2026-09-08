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
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.extension.*;

/**
 * JUnit 5 {@link ExecutionCondition} that disables multi-tenancy tests when no Axon Server license is present.
 * <p>
 * This condition is applied to the entire test class, so all tests in the class will be skipped if the condition is
 * not met.
 * <p>
 * Kept for nicer {@code fail()} when a dev has no license configured locally.
 */
public class DisableMultiTenancyTestsWithoutLicense implements ExecutionCondition {

    @Override
    @NonNull
    public ConditionEvaluationResult evaluateExecutionCondition(@NonNull ExtensionContext context) {
        return AxonServerTestInfrastructure.licenseExists()
                ? ConditionEvaluationResult.enabled("Axon Server license exists, multi-tenancy tests can run")
                : ConditionEvaluationResult.disabled("Axon Server license does not exist, multi-tenancy tests are disabled. If you want to run them, place a valid license under `/axon-server-test.license` in the classpath");
    }
}
