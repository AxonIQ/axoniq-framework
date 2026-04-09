/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */
package io.axoniq.framework.dataprotection.cryptoengine.jpa;

import io.axoniq.framework.dataprotection.cryptoengine.AbstractEngineTestSet;
import io.axoniq.framework.dataprotection.cryptoengine.CryptoEngine;
import io.axoniq.framework.dataprotection.utils.TestUtils;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.Persistence;

/**
 * Test class validating the {@link JpaCryptoEngine}.
 */
class JPATest extends AbstractEngineTestSet {

    private final static EntityManagerFactory emf = Persistence.createEntityManagerFactory("myPersistenceUnit");

    @Override
    protected CryptoEngine getCryptoEngine() {
        CryptoEngine cryptoEngine = new JpaCryptoEngine(emf);
        cryptoEngine.registerEntitlementManager(TestUtils.mockEntitlementManager());
        return cryptoEngine;
    }
}
