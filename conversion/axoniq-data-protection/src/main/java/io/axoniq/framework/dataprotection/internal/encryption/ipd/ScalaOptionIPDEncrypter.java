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
package io.axoniq.framework.dataprotection.internal.encryption.ipd;

import io.axoniq.framework.dataprotection.internal.encryption.core.Encrypter;
import io.axoniq.framework.dataprotection.internal.encryption.core.Operation;
import io.axoniq.framework.dataprotection.internal.model.PDField;

import java.util.Optional;
import javax.crypto.SecretKey;

/**
 * Implementation of {@link Encrypter} for Scala Option data. Processes the contained value
 * if present through the routing encrypter.
 *
 * @author Frans van Buul
 */
class ScalaOptionIPDEncrypter implements Encrypter<scala.Option> {

    private RoutingIPDEncrypter routingIPDEncrypter;

    public ScalaOptionIPDEncrypter(RoutingIPDEncrypter routingIPDEncrypter) {
        this.routingIPDEncrypter = routingIPDEncrypter;
    }

    public scala.Option process(PDField pdField, scala.Option input, Optional<SecretKey> key, Operation operation) {
        boolean objectsUnchanged = true;
        Object outputElement = null;

        if (input.isDefined()) {
            Object inputElement = input.get();
            outputElement = routingIPDEncrypter.process(pdField, inputElement, key, operation);
            if (inputElement != outputElement) {
                objectsUnchanged = false;
            }
        }

        return objectsUnchanged ? input : new scala.Some<>(outputElement);
    }
}
