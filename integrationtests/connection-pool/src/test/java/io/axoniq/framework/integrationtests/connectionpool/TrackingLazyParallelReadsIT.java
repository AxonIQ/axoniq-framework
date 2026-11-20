/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.integrationtests.connectionpool;

/**
 * Verifies parallel reads on top of the framework's tracking lazy data source.
 *
 * @author John Hendrikx
 */
class TrackingLazyParallelReadsIT extends AbstractParallelReadScenarios {

    @Override
    protected TransactionSetup smartSetup() {
        return TrackingLazySetups.TRACKING_LAZY;
    }
}
