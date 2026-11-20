/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.integrationtests.connectionpool;

/**
 * Verifies parallel reads with Hibernate and a JPA transaction manager on the tracking lazy data source.
 *
 * @author John Hendrikx
 */
class JpaTrackingLazyParallelReadsIT extends AbstractParallelReadScenarios {

    @Override
    protected TransactionSetup smartSetup() {
        return JpaSetups.JPA_TRACKING_LAZY;
    }
}
