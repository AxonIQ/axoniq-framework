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

import io.axoniq.framework.extension.dataprotection.DataProtectionAxoniqComponent;
import io.axoniq.framework.extension.dataprotection.internal.utils.ExceptionFactory;
import io.axoniq.license.entitlement.EntitlementManager;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Objects;
import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

/**
 * Partial implementation of {@link CryptoEngine} which operates using the JVM's standard
 * {@link java.security.Provider} to perform encryption and decryption, while assuming some separate
 * storage facility for keys. This could a SQL or NoSQL database, or a simple in memory structure for
 * testing purposes.
 * <p>
 * The core cryptographic functions of generating the {@link javax.crypto.SecretKey} and instantiating
 * the {@link javax.crypto.Cipher} are implemented by this class. Implementations are responsible for
 * providing some mechanism to store and retrieve the keys. The {@link javax.crypto.SecretKey} instances
 * being handled in this class are always of the subclass {@link SecretKeySpec} which means that it is
 * possible to access their byte encoding. Type key type is currently always AES en the length is always
 * 256 bits, but this may change in future versions and implementations may already take this into account.
 *
 * @author Frans van Buul
 */
public abstract class DatabaseBackedCryptoEngine implements CryptoEngine {

    private static final String TRANSFORM = "AES/CBC/PKCS5Padding";
    private static final String DIGEST_TRANSFORM = "AES/ECB/NoPadding";
    private static final String ALGORITHM = "AES";

    private KeyType keyType = KeyType.AES_256;
    private final ThreadLocal<SecureRandom> secureRandom = ThreadLocal.withInitial(SecureRandom::new);

    private EntitlementManager entitlementManager;

    /**
     * Validates that the data protection component is enabled in the license. This is called on every cryptographic
     * operation to ensure the license is valid. If the component is not enabled, an exception will be thrown on each
     * attempt.
     */
    protected void validateEntitlement() {
        entitlementManager.useAddon(DataProtectionAxoniqComponent.IDENTIFIER);
    }

    private SecretKeySpec generateKey() {
        int keyLengthBits;
        switch(keyType) {
            case AES_256: keyLengthBits = 256; break;
            case AES_192: keyLengthBits = 192; break;
            case AES_128: keyLengthBits = 128; break;
            default: throw ExceptionFactory.unexpectedKeyType(keyType);
        }
        byte[] keyBytes = new byte[keyLengthBits/8];
        secureRandom.get().nextBytes(keyBytes);
        return new SecretKeySpec(keyBytes, ALGORITHM);
    }

    @Override
    public SecretKey getOrCreateKey(String id) {
        validateEntitlement();
        SecretKey secretKey = getKey(id);
        if(secretKey == null) {
            secretKey = putKeyIfAbsent(id, generateKey());
        }
        return secretKey;
    }

    /**
     * Stores the key for the given id, if no key is currently registered for this id.
     * Returns the new key belonging to id, which is either the key that was already
     * registered or the key provided as the 2nd argument if no key was registered yet.
     * (Please note that this is different behaviour from
     * {@link java.util.concurrent.ConcurrentHashMap#putIfAbsent(Object, Object)}, which
     * always returns the prior value belonging to key.)
     *
     * @param id the id for which to store/retrieve the secret key
     * @param secretKeySpec the {@link SecretKeySpec} to store if none has been stored yet for id
     * @return the effective {@link SecretKeySpec} for id
     */
    protected abstract SecretKey putKeyIfAbsent(String id, SecretKeySpec secretKeySpec);

    @Override
    public Cipher createCipher() {
        try {
            return Cipher.getInstance(TRANSFORM);
        } catch (NoSuchAlgorithmException ex) {
            throw ExceptionFactory.forNoSuchAlg(ex, TRANSFORM);
        } catch (NoSuchPaddingException ex) {
            throw ExceptionFactory.forNoSuchPadding(ex, TRANSFORM);
        }
    }

    @Override
    public Cipher createDigestCipher() {
        try {
            return Cipher.getInstance(DIGEST_TRANSFORM);
        } catch (NoSuchAlgorithmException ex) {
            throw ExceptionFactory.forNoSuchAlg(ex, DIGEST_TRANSFORM);
        } catch (NoSuchPaddingException ex) {
            throw ExceptionFactory.forNoSuchPadding(ex, DIGEST_TRANSFORM
            );
        }
    }

    @Override
    public void setKeyType(KeyType keyType) {
        if(keyType == null) throw ExceptionFactory.forNullKeyType();
        this.keyType = keyType;
    }

    @Override
    public KeyType getKeyType() {
        return keyType;
    }

    @Override
    public void registerEntitlementManager(EntitlementManager entitlementManager) {
        this.entitlementManager = Objects.requireNonNull(entitlementManager, "The EntitlementManager must not be null");
    }
}
