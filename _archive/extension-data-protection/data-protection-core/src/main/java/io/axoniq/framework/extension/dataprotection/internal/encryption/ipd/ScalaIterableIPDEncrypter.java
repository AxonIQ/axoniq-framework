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
import io.axoniq.framework.extension.dataprotection.internal.utils.ExceptionFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.crypto.SecretKey;

/**
 * Implementation of {@link Encrypter} for Scala Iterable data. Processes each element of the iterable
 * through the routing encrypter, preserving the original collection type where possible.
 *
 * @author Frans van Buul
 */
class ScalaIterableIPDEncrypter implements Encrypter<scala.collection.Iterable> {

    private RoutingIPDEncrypter routingIPDEncrypter;

    public ScalaIterableIPDEncrypter(RoutingIPDEncrypter routingIPDEncrypter) {
        this.routingIPDEncrypter = routingIPDEncrypter;
    }

    public scala.collection.Iterable process(PDField pdField, scala.collection.Iterable input, Optional<SecretKey> key, Operation operation) {
        List<Object> output = new ArrayList<>(input.size());
        boolean objectsUnchanged = true;
        scala.collection.Iterator iterator = input.iterator();
        while(iterator.hasNext()) {
            Object inputElement = iterator.next();
            Object outputElement = routingIPDEncrypter.process(pdField, inputElement, key, operation);
            if(inputElement != outputElement) {
                objectsUnchanged = false;
            }
            output.add(outputElement);
        }
        if(objectsUnchanged) {
            return input;
        } else if(input instanceof scala.collection.mutable.Growable) {
            scala.collection.mutable.Growable growableInput = (scala.collection.mutable.Growable)input;
            growableInput.clear();
            for(Object outputElement : output) {
                growableInput.$plus$eq(outputElement);
            }
            return input;
        } else {
            scala.collection.mutable.Builder builder = input.newSpecificBuilder();
            for(Object outputElement : output) {
                builder.$plus$eq(outputElement);
            }
            Object outputCollection = builder.result();
            if(pdField.getField().getType().isInstance(outputCollection)) {
                return (scala.collection.Iterable)outputCollection;
            } else {
                throw ExceptionFactory.immutableCollection(pdField.getField());
            }
        }
    }

}
