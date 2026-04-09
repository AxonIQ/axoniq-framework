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
import io.axoniq.framework.dataprotection.internal.utils.CollectionUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import javax.crypto.SecretKey;

/**
 * Implementation of {@link Encrypter} for Collection data. Processes each element of the collection
 * through the routing encrypter, handling both mutable and immutable collections.
 *
 * @author Frans van Buul
 */
class CollectionIPDEncrypter implements Encrypter<Collection> {

    private RoutingIPDEncrypter routingIPDEncrypter;
    private boolean modifyImmutableCollections = true;

    public CollectionIPDEncrypter(RoutingIPDEncrypter routingIPDEncrypter) {
        this.routingIPDEncrypter = routingIPDEncrypter;
    }

    public Collection process(PDField pdField, Collection input, Optional<SecretKey> key, Operation operation) {
        List<Object> output = new ArrayList<>(input.size());
        boolean objectsChanged = false;
        for(Object inputElement : input) {
            Object outputElement = routingIPDEncrypter.process(pdField, inputElement, key, operation);
            if(inputElement != outputElement) {
                objectsChanged = true;
            }
            output.add(outputElement);
        }
        if(objectsChanged) {
            try {
                if (modifyImmutableCollections) {
                    CollectionUtils.replaceAll(input, output);
                } else {
                    input.clear();
                    input.addAll(output);
                }
            } catch (UnsupportedOperationException e) {
                // we've hit a collection that doesn't support modification
                Class<?> fieldType = pdField.getField().getType();
                return CollectionUtils.buildContainer(fieldType, output);
            }
        }
        return input;
    }

    public boolean isModifyImmutableCollections() {
        return modifyImmutableCollections;
    }

    public void setModifyImmutableCollections(boolean value) {
        modifyImmutableCollections = value;
    }

}
