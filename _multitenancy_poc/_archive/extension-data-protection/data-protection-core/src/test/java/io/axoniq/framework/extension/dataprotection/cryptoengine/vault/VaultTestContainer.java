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
package io.axoniq.framework.extension.dataprotection.cryptoengine.vault;

import org.testcontainers.vault.VaultContainer;

/**
 * Manages the lifecycle of a Vault test container used for integration tests.
 * This singleton class ensures that only one Vault container is created and shared
 * across all Vault-related tests.
 * <p>
 * The container is configured with:
 * <ul>
 *   <li>Development mode with root token "myroot"</li>
 *   <li>KV secrets engine (version 1 and 2)</li>
 *   <li>Transit secrets engine for encryption operations</li>
 * </ul>
 */
public class VaultTestContainer {

    /**
     * Root token used for Vault authentication in tests.
     */
    public static final String VAULT_TOKEN = "myroot";

    /**
     * Vault Docker image version.
     */
    private static final String VAULT_IMAGE = "hashicorp/vault:1.15";

    private static VaultContainer<?> container;

    /**
     * Gets or creates the singleton Vault container instance.
     * The container is lazily initialized on first access.
     *
     * @return the Vault container instance
     */
    public static synchronized VaultContainer<?> getContainer() {
        if (container == null) {
            container = new VaultContainer<>(VAULT_IMAGE)
                    .withVaultToken(VAULT_TOKEN)
                    .withInitCommand(
                            "secrets enable transit",
                            "secrets enable -version=1 -path=secret-v1 kv"
                    );
        }
        return container;
    }

    /**
     * Gets the HTTP address of the Vault container.
     *
     * @return the Vault HTTP address in format "http://host:port"
     */
    public static String getVaultAddress() {
        VaultContainer<?> vaultContainer = getContainer();
        return "http://" + vaultContainer.getHost() + ":" + vaultContainer.getMappedPort(8200);
    }

    /**
     * Gets the root token for Vault authentication.
     *
     * @return the root token
     */
    public static String getToken() {
        return VAULT_TOKEN;
    }

    private VaultTestContainer() {
        throw new Error("non-instantiable class");
    }
}
