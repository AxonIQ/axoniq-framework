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
package io.axoniq.framework.dataprotection.cryptoengine.vault;

import io.axoniq.framework.dataprotection.cryptoengine.AbstractEngineTestSet;
import io.axoniq.framework.dataprotection.cryptoengine.CryptoEngine;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.vault.VaultContainer;

import java.lang.invoke.MethodHandles;

/**
 * Tests of the VaultCryptoEngine using KV Secrets Engine API version 2.
 * <p>
 * This test automatically starts a Vault Docker container using Testcontainers.
 * The container is configured with development mode and the necessary secrets engines.
 * No manual Vault setup is required - tests will automatically start and stop
 * the Vault container.
 * <p>
 * The test uses the BetterCloud Vault client for authentication and policy management.
 * This client is exclusively used in the test code, not in the module's production code,
 * due to performance constraints.
 * <p>
 * Note: Docker must be running on the system for these tests to execute.
 */
@Testcontainers
public class VaultApiVersion2Test extends AbstractEngineTestSet {
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private static final int VAULT_ENGINE_VERSION = 2;

    private static VaultCryptoEngine vaultCryptoEngine;

    @Container
    static VaultContainer<?> vaultContainer = VaultTestContainer.getContainer();

    @org.junit.jupiter.api.BeforeAll
    static void initVault() {
        try {
            String vaultUrl = VaultTestContainer.getVaultAddress();
            String token = VaultTestContainer.getToken();
            vaultCryptoEngine = Utils.initVaultCryptoEngine(VAULT_ENGINE_VERSION, vaultUrl, token);
        } catch (Exception ex) {
            logger.error("Unable to configure Vault, Vault API 2 tests will be disabled.", ex);
            vaultCryptoEngine = null;
        }
    }

    @BeforeEach
    public void beforeMethod() {
        Assumptions.assumeTrue(vaultCryptoEngine != null);
    }

    protected CryptoEngine getCryptoEngine() {
        return vaultCryptoEngine;
    }
}
