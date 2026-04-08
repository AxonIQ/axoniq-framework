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

import java.io.OutputStream;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.util.Objects;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;

/**
 * Implementation of {@link CryptoEngine} that uses a JCE {@link KeyStore} implementation to store its keys. It will
 * examine the given {@link KeyStore} for its {@link java.security.Provider}, and then obtain a {@link KeyGenerator} and {@link Cipher} object from this
 * provider as well. Please note the default Java {@link KeyStore} implementation used for instance to store
 * certificates for a web server, is totally unsuitable for this particular application. The main reason why
 * this class exists is as a stepping stone to implement the PKCS#11 implementation {@link PKCS11CryptoEngine}.
 * <p>
 * Please note that the implementation will not call {@link KeyStore#store(KeyStore.LoadStoreParameter)} or
 * {@link KeyStore#store(OutputStream, char[])} after a call to {@link #getOrCreateKey(String)}. It assumes that
 * a {@link KeyStore} type is used that will save keys immediately, which is true of PKCS#11 key stores, but not
 * of a standard file-based key store.
 *
 * @author Frans van Buul
 */
public class JavaKeyStoreCryptoEngine implements CryptoEngine {

    private static final String TRANSFORM = "AES/CBC/PKCS5Padding";
    private static final String DIGEST_TRANSFORM = "AES/ECB/NoPadding";
    private static final String ALGORITHM = "AES";

    private final KeyStore keyStore;
    private final Lock createKeyLock = new ReentrantLock();
    private KeyType keyType = KeyType.AES_256;
    private ThreadLocal<KeyGenerator> keyGenerator = keyGenerator();

    private EntitlementManager entitlementManager;

    private ThreadLocal<KeyGenerator> keyGenerator() {
        int keyLengthBits;
        switch(keyType) {
            case AES_256: keyLengthBits = 256; break;
            case AES_192: keyLengthBits = 192; break;
            case AES_128: keyLengthBits = 128; break;
            default: throw ExceptionFactory.unexpectedKeyType(keyType);
        }
        return new ThreadLocal<KeyGenerator>() {
            @Override
            protected KeyGenerator initialValue() {
                try {
                    KeyGenerator keyGen = KeyGenerator.getInstance(ALGORITHM);
                    keyGen.init(keyLengthBits);
                    return keyGen;
                } catch (NoSuchAlgorithmException ex) {
                    throw ExceptionFactory.forNoSuchAlg(ex, ALGORITHM);
                }
            }
        };
    };

    /**
     * Constructs a new {@link JavaKeyStoreCryptoEngine}
     *
     * @param keyStore the {@link KeyStore} to use
     */
    public JavaKeyStoreCryptoEngine(KeyStore keyStore) {
        this.keyStore = keyStore;
    }

    /**
     * Validates that the data protection component is enabled in the license. This is called on every cryptographic
     * operation to ensure the license is valid. If the component is not enabled, an exception will be thrown on each
     * attempt.
     */
    private void validateEntitlement() {
        entitlementManager.useAddon(DataProtectionAxoniqComponent.IDENTIFIER);
    }

    @Override
    public SecretKey getOrCreateKey(String id) {
        validateEntitlement();
        SecretKey key = getKey(id);
        if(key == null) {
            createKeyLock.lock();
            try {
                key = getKey(id);
                if(key == null) {
                    SecretKey newKey = keyGenerator.get().generateKey();
                    keyStore.setKeyEntry(id, newKey, null, null);
                    key = newKey;
                }
            } catch(KeyStoreException ex) {
                throw ExceptionFactory.forKeyStoreException(ex);
            } finally {
                createKeyLock.unlock();
            }
        }
        return key;
    }

    @Override
    public SecretKey getKey(String id) {
        try {
            return (SecretKey)keyStore.getKey(id, null);
        } catch (KeyStoreException ex) {
            throw ExceptionFactory.forKeyStoreException(ex);
        } catch (NoSuchAlgorithmException ex) {
            throw ExceptionFactory.forNoSuchAlg(ex, "? (specified in key)");
        } catch (UnrecoverableKeyException ex) {
            throw ExceptionFactory.forUnrecoverableKeyException(ex);
        }
    }

    @Override
    public void deleteKey(String id) {
        try {
            keyStore.deleteEntry(id);
        } catch(KeyStoreException ex) {
            throw ExceptionFactory.forKeyStoreDeleteException(ex);
        }
    }

    @Override
    public Cipher createCipher() {
        try {
            return Cipher.getInstance(TRANSFORM, keyStore.getProvider());
        } catch (NoSuchAlgorithmException ex) {
            throw ExceptionFactory.forNoSuchAlg(ex, TRANSFORM);
        } catch (NoSuchPaddingException ex) {
            throw ExceptionFactory.forNoSuchPadding(ex, TRANSFORM);
        }
    }

    @Override
    public Cipher createDigestCipher() {
        try {
            return Cipher.getInstance(DIGEST_TRANSFORM, keyStore.getProvider());
        } catch (NoSuchAlgorithmException ex) {
            throw ExceptionFactory.forNoSuchAlg(ex, DIGEST_TRANSFORM);
        } catch (NoSuchPaddingException ex) {
            throw ExceptionFactory.forNoSuchPadding(ex, DIGEST_TRANSFORM);
        }
    }

    @Override
    public void setKeyType(KeyType keyType) {
        if(keyType == null) ExceptionFactory.forNullKeyType();
        this.keyType = keyType;
        this.keyGenerator = keyGenerator();
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
