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

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

/**
 * Default implementation of {@link KeyEntity}. This is a JPA {@link Entity} with a {@link Table} annotation
 * mapping it to a table named <code>axoniq_gdpr_keys</code>
 *
 * @author Frans van Buul
 */
@Entity
@Table(name = "axoniq_gdpr_keys")
public class DefaultKeyEntity implements KeyEntity {

    private String keyId;
    private String secretKeyBase64;

    protected DefaultKeyEntity() {
    }

    /**
     * {@inheritDoc}
     * Has a {@link Id} annotation identifying it as primary key and a {@link Column} annotation mapping it to
     * a column named <code>key_id</code>
     */
    @Override
    @Id
    @Column(name = "key_id")
    public String getKeyId() {
        return keyId;
    }

    @Override
    public void setKeyId(String keyId) {
        this.keyId = keyId;
    }

    /**
     * {@inheritDoc}
     * Has a {@link Column} annotation mapping it to a column named <code>secret_key</code>
     */
    @Override
    @Column(name = "secret_key")
    public String getSecretKeyBase64() {
        return secretKeyBase64;
    }

    @Override
    public void setSecretKeyBase64(String secretKeyBase64) {
        this.secretKeyBase64 = secretKeyBase64;
    }

}
