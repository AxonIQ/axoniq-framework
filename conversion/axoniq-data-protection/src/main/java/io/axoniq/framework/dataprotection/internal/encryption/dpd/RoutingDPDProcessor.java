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
package io.axoniq.framework.dataprotection.internal.encryption.dpd;

import io.axoniq.framework.dataprotection.api.ReplacementValueProvider;
import io.axoniq.framework.dataprotection.cryptoengine.CryptoEngine;
import io.axoniq.framework.dataprotection.internal.encryption.core.Operation;
import io.axoniq.framework.dataprotection.internal.model.ModelRegistry;
import org.axonframework.conversion.Converter;

import java.util.Collection;

import static io.axoniq.framework.dataprotection.internal.utils.ScalaDetector.isScalaPresent;

/**
 * Routes deep personal data processing to the appropriate type-specific processor based on the input data type.
 * Supports arrays, collections, Scala types, and regular objects.
 *
 * @author Frans van Buul
 */
public class RoutingDPDProcessor implements DPDProcessor<Object> {

    private final ArrayDPDProcessor arrayDPDProcessor;
    private final CollectionDPDProcessor collectionDPDProcessor;
    private final ScalaIterableDPDProcessor scalaIterableDPDProcessor;
    private final ScalaOptionDPDProcessor scalaOptionDPDProcessor;
    private final ObjectDPDProcessor objectDPDProcessor;

    /**
     * Creates a RoutingDPDProcessor using the Converter API (Axon Framework 5.x).
     * <p>
     * This is the recommended constructor for Axon Framework 5.x applications.
     *
     * @param modelRegistry the registry for field encryption models
     * @param cryptoEngine the crypto engine for key management
     * @param converter the converter for object serialization
     * @param replacementValueProvider the provider for replacement values
     * @since 5.0.0
     */
    public RoutingDPDProcessor(ModelRegistry modelRegistry,
                               CryptoEngine cryptoEngine,
                               Converter converter,
                               ReplacementValueProvider replacementValueProvider) {
        this.arrayDPDProcessor = new ArrayDPDProcessor(this);
        this.collectionDPDProcessor = new CollectionDPDProcessor(this);
        this.objectDPDProcessor = new ObjectDPDProcessor(
                modelRegistry, cryptoEngine, this, converter, replacementValueProvider
        );
        if (isScalaPresent()) {
            scalaIterableDPDProcessor = new ScalaIterableDPDProcessor(this);
            scalaOptionDPDProcessor = new ScalaOptionDPDProcessor(this);
        } else {
            scalaIterableDPDProcessor = null;
            scalaOptionDPDProcessor = null;
        }
    }

    @Override
    public Object process(Object input, EncryptionContext context, Operation operation) {
        if (input == null || context.hasBeenProcessed(input)) {
            return input;
        }

        context.registerAsProcessed(input);
        Object result;

        if (input instanceof Collection) {
            result = collectionDPDProcessor.process((Collection) input, context, operation);
        } else if (scalaIterableDPDProcessor != null && input instanceof scala.collection.Iterable) {
            result = scalaIterableDPDProcessor.process((scala.collection.Iterable) input, context, operation);
        } else if (input.getClass().isArray() && !input.getClass().getComponentType().isPrimitive()) {
            result = arrayDPDProcessor.process((Object[]) input, context, operation);
        } else if (scalaOptionDPDProcessor != null && input instanceof scala.Option) {
            result = scalaOptionDPDProcessor.process((scala.Option) input, context, operation);
        } else {
            result = objectDPDProcessor.process(input, context, operation);
        }

        return result;
    }

    public boolean isModifyImmutableCollections() {
        return objectDPDProcessor.isModifyImmutableCollections();
    }

    public void setModifyImmutableCollections(boolean value) {
        objectDPDProcessor.setModifyImmutableCollections(value);
    }
}
