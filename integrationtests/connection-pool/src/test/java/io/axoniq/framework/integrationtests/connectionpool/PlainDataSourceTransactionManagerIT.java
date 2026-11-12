/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.integrationtests.connectionpool;

/**
 * Runs the scenarios with a plain pool and a Spring {@code DataSourceTransactionManager}, which takes its connection
 * when the transaction begins.
 *
 * @author John Hendrikx
 */
class PlainDataSourceTransactionManagerIT extends AbstractSingleConnectionScenarios {

    @Override
    protected TransactionSetup setup() {
        return SpringJdbcSetups.PLAIN;
    }
}
