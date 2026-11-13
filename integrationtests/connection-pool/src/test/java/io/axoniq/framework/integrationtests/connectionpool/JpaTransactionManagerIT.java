/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.integrationtests.connectionpool;

/**
 * Runs the scenarios with a Spring {@code JpaTransactionManager} using Hibernate.
 *
 * @author John Hendrikx
 */
class JpaTransactionManagerIT extends AbstractSingleConnectionScenarios {

    @Override
    protected TransactionSetup setup() {
        return JpaSetups.JPA_TRANSACTION_MANAGER;
    }
}
