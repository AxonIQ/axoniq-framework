/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.integrationtests.connectionpool;

/**
 * Runs the scenarios with the common setup, but with a pool that hands out its connections with auto-commit turned
 * off, as many production pools are configured.
 *
 * @author John Hendrikx
 */
class PlainAutoCommitOffIT extends AbstractSingleConnectionScenarios {

    @Override
    protected TransactionSetup setup() {
        return SpringJdbcSetups.PLAIN;
    }

    @Override
    protected boolean autoCommit() {
        return false;
    }
}
