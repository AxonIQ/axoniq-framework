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
package io.axoniq.framework.extension.dataprotection.internal.encryption.ipd;

import io.axoniq.framework.extension.dataprotection.api.ReplacementValueProvider;
import io.axoniq.framework.extension.dataprotection.cryptoengine.CryptoEngine;
import io.axoniq.framework.extension.dataprotection.internal.encryption.core.ByteArrayEncrypter;
import io.axoniq.framework.extension.dataprotection.internal.encryption.core.Encrypter;
import io.axoniq.framework.extension.dataprotection.internal.encryption.core.Operation;
import io.axoniq.framework.extension.dataprotection.internal.model.PDField;
import io.axoniq.framework.extension.dataprotection.internal.utils.ExceptionFactory;

import java.util.Collection;
import java.util.Optional;
import javax.crypto.SecretKey;

import static io.axoniq.framework.extension.dataprotection.internal.utils.ScalaDetector.isScalaPresent;

/**
 * Routes encryption operations to the appropriate type-specific encrypter based on the input data type.
 * Supports String, byte[], Collection, Array, and Scala types.
 *
 * @author Frans van Buul
 */
public class RoutingIPDEncrypter implements Encrypter<Object> {

    private ByteArrayEncrypter byteArrayEncrypter;
    private StringIPDEncrypter stringIPDEncrypter;
    private CollectionIPDEncrypter collectionIPDEncrypter;
    private ArrayIPDEncrypter arrayIPDEncrypter;
    private ScalaIterableIPDEncrypter scalaIterableIPDEncrypter;
    private ScalaOptionIPDEncrypter scalaOptionIPDEncrypter;

    public RoutingIPDEncrypter(CryptoEngine cryptoEngine, ReplacementValueProvider replacementValueProvider) {
        this.byteArrayEncrypter = new ByteArrayEncrypter(cryptoEngine, replacementValueProvider);
        this.stringIPDEncrypter = new StringIPDEncrypter(byteArrayEncrypter, replacementValueProvider);
        this.collectionIPDEncrypter = new CollectionIPDEncrypter(this);
        this.arrayIPDEncrypter = new ArrayIPDEncrypter(this);
        if (isScalaPresent()) {
            scalaIterableIPDEncrypter = new ScalaIterableIPDEncrypter(this);
            scalaOptionIPDEncrypter = new ScalaOptionIPDEncrypter(this);
        } else {
            scalaIterableIPDEncrypter = null;
            scalaOptionIPDEncrypter = null;
        }
    }

    public Object process(PDField pdField, Object input, Optional<SecretKey> key, Operation operation) {
        if (input == null) {
            return null;
        } else if (input instanceof String) {
            return stringIPDEncrypter.process(pdField, (String) input, key, operation);
        } else if (input.getClass().isArray() && input.getClass().getComponentType().equals(byte.class)) {
            return byteArrayEncrypter.process(pdField, (byte[]) input, key, operation);
        } else if (input instanceof Collection) {
            return collectionIPDEncrypter.process(pdField, (Collection) input, key, operation);
        } else if (scalaIterableIPDEncrypter != null && input instanceof scala.collection.Iterable) {
            return scalaIterableIPDEncrypter.process(pdField, (scala.collection.Iterable) input, key, operation);
        } else if (input.getClass().isArray() && !input.getClass().getComponentType().isPrimitive()) {
            return arrayIPDEncrypter.process(pdField, (Object[]) input, key, operation);
        } else if (scalaOptionIPDEncrypter != null && input instanceof scala.Option) {
            return scalaOptionIPDEncrypter.process(pdField, (scala.Option) input, key, operation);
        } else {
            throw ExceptionFactory.wrongTypeForIPD(input.getClass());
        }
    }

    public boolean isModifyImmutableCollections() {
        return collectionIPDEncrypter.isModifyImmutableCollections();
    }

    public void setModifyImmutableCollections(boolean value) {
        collectionIPDEncrypter.setModifyImmutableCollections(value);
    }
}
