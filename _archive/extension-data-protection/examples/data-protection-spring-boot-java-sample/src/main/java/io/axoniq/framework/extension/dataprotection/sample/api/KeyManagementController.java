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
package io.axoniq.framework.extension.dataprotection.sample.api;

import io.axoniq.framework.extension.dataprotection.cryptoengine.CryptoEngine;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;
import javax.crypto.SecretKey;

/**
 * REST controller for managing encryption keys in the InMemoryCryptoEngine.
 * Useful for debugging and testing data protection functionality.
 *
 */
@RestController
@RequestMapping("/api/keys")
public class KeyManagementController {

    private final CryptoEngine cryptoEngine;

    public KeyManagementController(CryptoEngine cryptoEngine) {
        this.cryptoEngine = cryptoEngine;
    }

    /**
     * Check if a specific key exists.
     *
     * @param keyId the key ID to check
     * @return Map indicating if key exists
     */
    @GetMapping("/{keyId}")
    public Map<String, Object> checkKey(@PathVariable String keyId) {
        SecretKey key = cryptoEngine.getKey(keyId);

        Map<String, Object> response = new HashMap<>();
        response.put("keyId", keyId);
        response.put("exists", key != null);
        if (key != null) {
            response.put("algorithm", key.getAlgorithm());
            response.put("format", key.getFormat());
        }
        return response;
    }

    /**
     * Delete a specific key from memory.
     * USE WITH CAUTION: Deleting a key will make encrypted data with that key unreadable!
     *
     * @param keyId the key ID to delete
     * @return Map with deletion result
     */
    @DeleteMapping("/{keyId}")
    public Map<String, Object> deleteKey(@PathVariable String keyId) {
        boolean existedBefore = cryptoEngine.getKey(keyId) != null;

        cryptoEngine.deleteKey(keyId);

        boolean existsAfter = cryptoEngine.getKey(keyId) != null;

        Map<String, Object> response = new HashMap<>();
        response.put("keyId", keyId);
        response.put("existedBefore", existedBefore);
        response.put("deleted", existedBefore && !existsAfter);

        return response;
    }
}
