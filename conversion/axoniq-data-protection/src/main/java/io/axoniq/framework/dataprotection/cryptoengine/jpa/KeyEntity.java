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

/**
 * Interface to describe a key entity managed by the {@link JpaCryptoEngine}. The module provides a default
 * implementation {@link DefaultKeyEntity}, but this is specified as an interface so applications may easily
 * use a different implementation, with their own JPA annotations.
 * <p>
 * Implementations are required to have a zero-arg constructor (which is generally required for JPA entities).
 * The {@link JpaCryptoEngine} will create new instances by invoking this constructor and then invoking
 * the setters.
 *
 * @author Frans van Buul
 */
public interface KeyEntity {

    /**
     * Read accessor to the <code>keyId</code>. This is the primary key of the entity.
     * @return the current value
     */
    String getKeyId();

    /**
     * Write accessor to the <code>keyId</code>. This is the primary key of the entity.
     * @param keyId the new value
     */
    void setKeyId(String keyId);

    /**
     * Read accessor to <code>secretKeyBase64</code>. This is a representation of the secret key bytes
     * in Base64-form.
     * @return the current value
     */
    String getSecretKeyBase64();

    /**
     * Write accessor to <code>secretKeyBase64</code>. This is a representation of the secret key bytes
     * in Base64-form.
     * @param secretKeyBase64 the new value
     */
    void setSecretKeyBase64(String secretKeyBase64);
}