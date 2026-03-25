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
package io.axoniq.framework.extension.dataprotection.api;

import io.axoniq.framework.extension.dataprotection.cryptoengine.CryptoEngine;
import io.axoniq.framework.extension.dataprotection.internal.encryption.EncryptionProcessor;
import io.axoniq.framework.extension.dataprotection.internal.encryption.core.Operation;
import org.axonframework.conversion.Converter;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class can take an object and encrypt and decrypt its fields, including the fields of any objects deeper in
 * the object tree, according to the configuration of the various Axon Data Protection Module annotations.
 * To accomplish this, it needs the capabilities of a {@link CryptoEngine}.
 *
 * @author Frans van Buul
 */
public final class FieldEncrypter {
    private final EncryptionProcessor encryptionProcessor;
    private Set<String> groups = null;

    // ===== Factory methods (Axon Framework 5.x - Converter API) =====

    /**
     * Creates a FieldEncrypter using the Converter API (Axon Framework 5.x) with default {@link ReplacementValueProvider}.
     * <p>
     * This is the recommended factory method for Axon Framework 5.x applications.
     *
     * @param cryptoEngine the {@link CryptoEngine} to be used
     * @param converter the {@link Converter} to be used for serialization
     * @return a new FieldEncrypter instance
     * @since 5.0.0
     */
    public static FieldEncrypter create(CryptoEngine cryptoEngine, Converter converter) {
        return new FieldEncrypter(cryptoEngine, converter, new ReplacementValueProvider());
    }

    /**
     * Creates a FieldEncrypter using the Converter API (Axon Framework 5.x) with custom {@link ReplacementValueProvider}.
     * <p>
     * This is the recommended factory method for Axon Framework 5.x applications.
     *
     * @param cryptoEngine the {@link CryptoEngine} to be used
     * @param converter the {@link Converter} to be used for serialization
     * @param replacementValueProvider a (custom) replacement value provider
     * @return a new FieldEncrypter instance
     * @since 5.0.0
     */
    public static FieldEncrypter create(CryptoEngine cryptoEngine, Converter converter, ReplacementValueProvider replacementValueProvider) {
        return new FieldEncrypter(cryptoEngine, converter, replacementValueProvider);
    }

    // ===== Constructors (Axon Framework 5.x - Converter API) =====

    /**
     * Instantiates a FieldEncrypter using the Converter API (Axon Framework 5.x) with default {@link ReplacementValueProvider}.
     * <p>
     * Consider using the static factory method {@link #create(CryptoEngine, Converter)} instead.
     *
     * @param cryptoEngine the {@link CryptoEngine} to be used
     * @param converter the {@link Converter} to be used for serialization
     * @since 5.0.0
     */
    public FieldEncrypter(CryptoEngine cryptoEngine, Converter converter) {
        this.encryptionProcessor = new EncryptionProcessor(cryptoEngine, converter, new ReplacementValueProvider());
    }

    /**
     * Instantiates a FieldEncrypter using the Converter API (Axon Framework 5.x) with custom {@link ReplacementValueProvider}.
     * <p>
     * Consider using the static factory method {@link #create(CryptoEngine, Converter, ReplacementValueProvider)} instead.
     *
     * @param cryptoEngine the {@link CryptoEngine} to be used
     * @param converter the {@link Converter} to be used for serialization
     * @param replacementValueProvider a (custom) replacement value provider
     * @since 5.0.0
     */
    public FieldEncrypter(CryptoEngine cryptoEngine, Converter converter, ReplacementValueProvider replacementValueProvider) {
        this.encryptionProcessor = new EncryptionProcessor(cryptoEngine, converter, replacementValueProvider);
    }

    /**
     * Encrypts the fields of the object.
     * For mutable objects, modifies in-place. For immutable objects (records), returns a new instance.
     * The result object may be different from the input object for immutable types.
     *
     * @param obj the object to encrypt
     */
    public void encrypt(Object obj) {
        // For backward compatibility, keep void return type
        // But note: for immutable objects (records), the encrypted object is returned by process() but ignored here
        encryptionProcessor.process(obj, Operation.ENCRYPT, Collections.emptyMap(), groups);
    }

    /**
     * Encrypts the fields of the object and returns the result.
     * For mutable objects, modifies in-place and returns the same instance.
     * For immutable objects (records), creates and returns a new instance with encrypted fields.
     *
     * @param obj the object to encrypt
     * @param <T> the type of the object
     * @return the encrypted object (same instance for mutable objects, new instance for immutable objects)
     * @since 5.0.0
     */
    @SuppressWarnings("unchecked")
    public <T> T encryptAndReturn(T obj) {
        return (T) encryptionProcessor.process(obj, Operation.ENCRYPT, Collections.emptyMap(), groups);
    }

    /**
     * Encrypts the fields of the object.
     * @param obj the object to encrypt
     * @param keyId the id of key to be preloaded in the encryption context with default group
     */
    public void encrypt(Object obj, String keyId) {
        encryptionProcessor.process(obj, Operation.ENCRYPT, Collections.singletonMap("", keyId), groups);
    }

    /**
     * Encrypts the fields of the object.
     * @param obj the object to encrypt
     * @param keyIds a map of group/keyId pairs defining keys to be preloaded into the encryption context
     */
    public void encrypt(Object obj, Map<String, String> keyIds) {
        encryptionProcessor.process(obj, Operation.ENCRYPT, keyIds, groups);
    }

    /**
     * Encrypts the fields of the object, restricting to certain groups.
     * @param obj the object to encrypt
     * @param groups the field groups to process. This override the 'groups' property of the FieldEncrypter object.
     */
    public void encrypt(Object obj, Set<String> groups) {
        encryptionProcessor.process(obj, Operation.ENCRYPT, Collections.emptyMap(), groups);
    }

    /**
     * Encrypts the fields of the object, restricting to certain groups.
     * @param obj the object to encrypt
     * @param keyId the id of key to be preloaded in the encryption context with default group
     * @param groups the field groups to process. This override the 'groups' property of the FieldEncrypter object.
     */
    public void encrypt(Object obj, String keyId, Set<String> groups) {
        encryptionProcessor.process(obj, Operation.ENCRYPT, Collections.singletonMap("", keyId), groups);
    }

    /**
     * Encrypts the fields of the object, restricting to certain groups.
     * @param obj the object to encrypt
     * @param keyIds a map of group/keyId pairs defining keys to be preloaded into the encryption context
     * @param groups the field groups to process. This override the 'groups' property of the FieldEncrypter object.
     */
    public void encrypt(Object obj, Map<String, String> keyIds, Set<String> groups) {
        encryptionProcessor.process(obj, Operation.ENCRYPT, keyIds, groups);
    }

    /**
     * Decrypts the fields of the object.
     * For mutable objects, modifies in-place. For immutable objects (records), returns a new instance.
     * The result object may be different from the input object for immutable types.
     *
     * @param obj the object to decrypt
     */
    public void decrypt(Object obj) {
        // For backward compatibility, keep void return type
        // But note: for immutable objects (records), the decrypted object is returned by process() but ignored here
        encryptionProcessor.process(obj, Operation.DECRYPT, Collections.emptyMap(), groups);
    }

    /**
     * Decrypts the fields of the object and returns the result.
     * For mutable objects, modifies in-place and returns the same instance.
     * For immutable objects (records), creates and returns a new instance with decrypted fields.
     *
     * @param obj the object to decrypt
     * @param <T> the type of the object
     * @return the decrypted object (same instance for mutable objects, new instance for immutable objects)
     * @since 5.0.0
     */
    @SuppressWarnings("unchecked")
    public <T> T decryptAndReturn(T obj) {
        return (T) encryptionProcessor.process(obj, Operation.DECRYPT, Collections.emptyMap(), groups);
    }

    /**
     * Decrypts the fields of the object.
     * @param obj the object to decrypt
     * @param keyId the id of key to be preloaded in the decryption context with default group
     */
    public void decrypt(Object obj, String keyId) {
        encryptionProcessor.process(obj, Operation.DECRYPT, Collections.singletonMap("", keyId), groups);
    }

    /**
     * Decrypts the fields of the object.
     * @param obj the object to decrypt
     * @param keyIds a map of group/keyId pairs defining keys to be preloaded into the decryption context
     */
    public void decrypt(Object obj, Map<String, String> keyIds) {
        encryptionProcessor.process(obj, Operation.DECRYPT, keyIds, groups);
    }

    /**
     * Decrypts the fields of the object, restricting to certain groups.
     * @param obj the object to decrypt
     * @param groups the field groups to process. This override the 'groups' property of the FieldEncrypter object.
     */
    public void decrypt(Object obj, Set<String> groups) {
        encryptionProcessor.process(obj, Operation.DECRYPT, Collections.emptyMap(), groups);
    }

    /**
     * Decrypts the fields of the object, restricting to certain groups.
     * @param obj the object to decrypt
     * @param keyId the id of key to be preloaded in the decryption context with default group
     * @param groups the field groups to process. This override the 'groups' property of the FieldEncrypter object.
     */
    public void decrypt(Object obj, String keyId, Set<String> groups) {
        encryptionProcessor.process(obj, Operation.DECRYPT, Collections.singletonMap("", keyId), groups);
    }

    /**
     * Decrypts the fields of the object, restricting to certain groups.
     * @param obj the object to decrypt
     * @param keyIds a map of group/keyId pairs defining keys to be preloaded into the decryption context
     * @param groups the field groups to process. This override the 'groups' property of the FieldEncrypter object.
     */
    public void decrypt(Object obj, Map<String, String> keyIds, Set<String> groups) {
        encryptionProcessor.process(obj, Operation.DECRYPT, keyIds, groups);
    }

    /**
     * Directly replaces the values of the fields of the object by the value they would get if they would
     * get crypto-deleted, without doing actual encryption or decryption.
     * @param obj the object to replace
     */
    public void replace(Object obj) {
        encryptionProcessor.process(obj, Operation.REPLACE, Collections.emptyMap(), groups);
    }

    /**
     * Directly replaces the values of the fields of the object and returns the result.
     * For mutable objects, modifies in-place and returns the same instance.
     * For immutable objects (records), creates and returns a new instance with replaced fields.
     *
     * @param obj the object to replace
     * @param <T> the type of the object
     * @return the object with replaced values (same instance for mutable objects, new instance for immutable objects)
     * @since 5.0.0
     */
    @SuppressWarnings("unchecked")
    public <T> T replaceAndReturn(T obj) {
        return (T) encryptionProcessor.process(obj, Operation.REPLACE, Collections.emptyMap(), groups);
    }

    /**
     * Directly replaces the values of the fields of the object by the value they would get if they would
     * get crypto-deleted, without doing actual encryption or decryption.
     * @param obj the object to replace
     * @param groups the field groups to process. This override the 'groups' property of the FieldEncrypter object.
     */
    public void replace(Object obj, Set<String> groups) {
        encryptionProcessor.process(obj, Operation.REPLACE, Collections.emptyMap(), groups);
    }

    /**
     * Examines whether object may change under encryption/decryption. This is the case if the argument
     * is non-<code>null</code> and the object's class (or superclass) has one or more Axon Data Protection Module
     * annotations.
     *
     * @param obj the object to examine
     * @return <code>true</code> if the object may get encrypted
     */
    public boolean willProcess(Object obj) {
        return encryptionProcessor.willProcess(obj);
    }

    /**
     * Read accessor for the modifyImmutableCollections property. This property determines whether the
     * module will attempt to modify immutable collections such as those returned by {@link java.util.Collections#unmodifiableList(List)}.
     * <code>true</code> by default.
     *
     * @return the current modifyImmutableCollections setting
     */
    public boolean isModifyImmutableCollections() {
        return encryptionProcessor.isModifyImmutableCollections();
    }

    /**
     * Write accessor for the modifyImmutableCollections property. This property determines whether the
     * module will attempt to modify immutable collections such as those returned by {@link java.util.Collections#unmodifiableList(List)}.
     * <code>true</code> by default.
     *
     * @param value the new value of the property
     */
    public void setModifyImmutableCollections(boolean value) {
        encryptionProcessor.setModifyImmutableCollections(value);
    }

    /**
     * Read accessor for the groups property. This property determines which field groups will be processed
     * by default by the encrypt/decrypt methods. Initially, this value is <code>null</code> which means that all
     * groups will be processed.
     *
     * @return the current value of the groups property. Reference is copied directly, no defensive cloning or
     *     immutable wrapping.
     */
    public Set<String> getGroups() {
        return groups;
    }

    /**
     * Write accessor for the groups property. This property determines which field groups will be processed
     * by default by the encrypt/decrypt methods. Initially, this value is <code>null</code> which means that all
     * groups will be processed.
     *
     * @param groups the new value of the groups property. Reference is copied directly, no defensive cloning.
     */
    public void setGroups(Set<String> groups) {
        this.groups = groups;
    }

}
