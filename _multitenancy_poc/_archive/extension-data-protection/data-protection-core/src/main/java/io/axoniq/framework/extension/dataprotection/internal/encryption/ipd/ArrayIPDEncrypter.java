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

import io.axoniq.framework.extension.dataprotection.internal.encryption.core.Encrypter;
import io.axoniq.framework.extension.dataprotection.internal.encryption.core.Operation;
import io.axoniq.framework.extension.dataprotection.internal.model.PDField;

import java.util.Optional;
import javax.crypto.SecretKey;

/**
 * Implementation of {@link Encrypter} for array data. Processes each element of the array
 * through the routing encrypter.
 *
 * @author Frans van Buul
 */
class ArrayIPDEncrypter implements Encrypter<Object[]> {

    private RoutingIPDEncrypter routingIPDEncrypter;

    public ArrayIPDEncrypter(RoutingIPDEncrypter routingIPDEncrypter) {
        this.routingIPDEncrypter = routingIPDEncrypter;
    }

    public Object[] process(PDField pdField, Object[] input, Optional<SecretKey> key, Operation operation) {
        for(int i = 0; i < input.length; i++) {
            input[i] = routingIPDEncrypter.process(pdField, input[i], key, operation);
        }
        return input;
    }

}
