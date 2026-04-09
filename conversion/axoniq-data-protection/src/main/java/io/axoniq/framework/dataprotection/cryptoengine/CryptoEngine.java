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
package io.axoniq.framework.dataprotection.cryptoengine;

import io.axoniq.license.entitlement.EntitlementManager;

import java.security.spec.AlgorithmParameterSpec;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;

/**
 * Interface towards the cryptographic functionality needed by the Axon Data Protection Module. The module provides
 * functionality for retrieval, generation/storage and deletion of AES-256 keys, and obtaining a Cipher object to
 * perform encryption and decryption using a {@link javax.crypto.SecretKey}. The Axon Data Protection Module offers
 * various implementations for this interface, including an in-memory implementation for test, an implementation that
 * stored keys in a database, and an implementation that works with a hardware security module (HSM).
 * <p>
 * Both the {@link javax.crypto.SecretKey} management functionality and obtaining a {@link javax.crypto.Cipher} must be
 * in a single interface to support those cases where HSMs are used. In those cases, a SecretKey object won't contain
 * any actual key material. Instead it's just a reference to a secret key stored on the device. The Cipher object
 * interfacing to the HSM will be able to deal with this.
 * <p>
 * In principle, we could have directly used the {@link java.security.Provider} and {@link java.security.KeyStore}
 * abstractions instead of this one. This would have led to unnecessary complexity in the most common use case where
 * keys are stored in a regular database. Therefore, this simpler abstraction is offered instead.
 * <p>
 * Implementations of this class are responsible for selecting the symmetric encryption algorithm, mode, padding, and
 * key length. All standard implementations in the Axon Data Protection Module use AES in CBC mode with PKCS#5 padding
 * and a 256-bit key length.
 *
 * @author Frans van Buul
 * @see InMemoryCryptoEngine
 * @see JavaKeyStoreCryptoEngine
 */
public interface CryptoEngine {

    /**
     * Retrieves the key for a given id. If no such key is registered, generates a new random key and stores it under
     * the alias before returning it.
     *
     * @param id the id of the {@link javax.crypto.SecretKey}
     * @return the potentially new {@link javax.crypto.SecretKey} associated with the id
     */
    SecretKey getOrCreateKey(String id);

    /**
     * Retrieves the key for a given id.
     *
     * @param id the id of the {@link javax.crypto.SecretKey}
     * @return the existing {@link javax.crypto.SecretKey} associated with the id, or {@code null} if no such key exists
     */
    SecretKey getKey(String id);

    /**
     * Deletes the key for a given id. Does nothing if the key doesn't exist.
     *
     * @param id the id of the {@link javax.crypto.SecretKey}
     */
    void deleteKey(String id);

    /**
     * Creates an uninitialized {@link javax.crypto.Cipher} instance for the correct transformation (AES, CBC, PKCS#5)
     * and provider. Clients should still call the
     * {@link javax.crypto.Cipher#init(int, java.security.Key, AlgorithmParameterSpec)} to specify operation mode
     * (encryption or decryption), key and initialization vector.
     *
     * @return the {@link javax.crypto.Cipher}
     */
    Cipher createCipher();

    /**
     * Creates an uninitialized {@link javax.crypto.Cipher} instance for calculating the encrypted digest. For this
     * specific purpose, it should use AES, EBC and no padding, and the same provider as for the other operations.
     * Clients should still call the {@link javax.crypto.Cipher#init(int, java.security.Key)} to specify operation mode
     * (always encryption) and key.
     *
     * @return the {@link javax.crypto.Cipher}
     */
    Cipher createDigestCipher();

    /**
     * Sets the {@link KeyType} to use, which determines the length of newly generated keys. Defaults to
     * {@link KeyType#AES_256} if not set.
     *
     * @param keyType the new {@link KeyType}
     */
    void setKeyType(KeyType keyType);

    /**
     * Retrieves the currently used {@link KeyType} for new keys
     *
     * @return the current value
     */
    KeyType getKeyType();

    /**
     * Registers the given {@code entitlementManager} with this {@code CryptoEngine}, required to ensure a usable
     * license is present
     *
     * @param entitlementManager the {@code EntitlementManager} used to validate if a usable license is present
     */
    void registerEntitlementManager(EntitlementManager entitlementManager);
}
