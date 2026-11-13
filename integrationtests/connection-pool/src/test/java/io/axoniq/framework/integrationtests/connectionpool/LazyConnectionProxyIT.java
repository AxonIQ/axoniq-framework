/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.integrationtests.connectionpool;

/**
 * Runs the scenarios with a Spring {@code DataSourceTransactionManager} on a {@code LazyConnectionDataSourceProxy},
 * which only takes a real connection when the first statement runs.
 *
 * @author John Hendrikx
 */
class LazyConnectionProxyIT extends AbstractSingleConnectionScenarios {

    @Override
    protected TransactionSetup setup() {
        return SpringJdbcSetups.LAZY_PROXY;
    }
}
