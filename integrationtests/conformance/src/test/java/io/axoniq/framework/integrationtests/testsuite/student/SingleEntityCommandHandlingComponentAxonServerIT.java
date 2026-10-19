/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.integrationtests.testsuite.student;

import io.axoniq.framework.integrationtests.testsuite.infrastructure.AxonServerTestInfrastructure;
import org.axonframework.integrationtests.testsuite.infrastructure.TestInfrastructure;
import org.axonframework.integrationtests.testsuite.student.SingleEntityCommandHandlingComponentIT;

/**
 * Runs {@link org.axonframework.integrationtests.testsuite.student.SingleEntityCommandHandlingComponentIT} against a
 * real Axon Server instance.
 */
public class SingleEntityCommandHandlingComponentAxonServerIT extends SingleEntityCommandHandlingComponentIT {

    private static final TestInfrastructure INFRASTRUCTURE = new AxonServerTestInfrastructure();

    @Override
    protected TestInfrastructure testInfrastructure() {
        return INFRASTRUCTURE;
    }
}
