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
package io.axoniq.framework.extension.dataprotection.cryptoengine;

import io.axoniq.framework.extension.dataprotection.utils.TestUtils;
import io.axoniq.license.entitlement.EntitlementManager;
import org.junit.jupiter.api.*;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.UUID;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;

import static org.junit.jupiter.api.Assertions.*;

public abstract class AbstractEngineTestSet {

    private CryptoEngine cryptoEngine;

    @BeforeEach
    public void init() {
        cryptoEngine = getCryptoEngine();
    }

    protected abstract CryptoEngine getCryptoEngine();

    /**
     * Creates an EntitlementManager for testing purposes.
     * <p>
     * This returns an EnforcingEntitlementManager with a no-op LicenseSource.
     * The manager starts in a 1-hour grace period during which all components are allowed.
     */
    protected static EntitlementManager mockEntitlementManager() {
        return TestUtils.mockEntitlementManager();
    }

    @Test
    public void repeatedInvocationsWithSameIdGiveSameResult() {
        String id1 = UUID.randomUUID().toString();
        SecretKey key1 = cryptoEngine.getOrCreateKey(id1);
        SecretKey key2 = cryptoEngine.getOrCreateKey(id1);
        assertTrue(keysEquivalent(key1, key2));
    }

    @Test
    public void repeatedInvocationsWithDifferentIdGiveDifferentResult() {
        String id1 = UUID.randomUUID().toString();
        String id2 = UUID.randomUUID().toString();
        SecretKey key1 = cryptoEngine.getOrCreateKey(id1);
        SecretKey key2 = cryptoEngine.getOrCreateKey(id2);
        assertFalse(keysEquivalent(key1, key2));
    }

    @Test
    public void getDoesNotCreateKeys() {
        String id1 = UUID.randomUUID().toString();
        SecretKey key1 = cryptoEngine.getKey(id1);
        assertNull(key1);
    }

    @Test
    public void keyCanBeDeleted() {
        String id1 = UUID.randomUUID().toString();
        SecretKey key1 = cryptoEngine.getOrCreateKey(id1);
        assertNotNull(key1);
        cryptoEngine.deleteKey(id1);
        SecretKey key2 = cryptoEngine.getKey(id1);
        assertNull(key2);
    }

    boolean keysEquivalent(SecretKey key1, SecretKey key2) {
        try {
            byte[] clearBytes = "This is a test".getBytes(StandardCharsets.UTF_8);
            byte[] iv = new byte[16];
            Cipher encCipher = cryptoEngine.createCipher();
            encCipher.init(Cipher.ENCRYPT_MODE, key1, new IvParameterSpec(iv));
            byte[] cryptoBytes = encCipher.doFinal(clearBytes);
            Cipher decCipher = cryptoEngine.createCipher();
            decCipher.init(Cipher.DECRYPT_MODE, key2, new IvParameterSpec(iv));
            try {
                byte[] decryptedBytes = decCipher.doFinal(cryptoBytes);
                return Arrays.equals(clearBytes, decryptedBytes);
            } catch(Exception ex) {
                return false;
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
