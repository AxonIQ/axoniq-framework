/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
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
                : ConditionEvaluationResult.disabled("Axon Server license does not exist, multi-tenancy tests are disabled. If you want to run them, set the `AXONIQ_LICENSE` environment variable to the content of a valid license, or place one under `/axon-server-test.license` in the classpath");
    }
}
