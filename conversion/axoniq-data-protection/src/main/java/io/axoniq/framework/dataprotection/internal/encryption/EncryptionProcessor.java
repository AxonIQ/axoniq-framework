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
package io.axoniq.framework.dataprotection.internal.encryption;

import io.axoniq.framework.dataprotection.api.ReplacementValueProvider;
import io.axoniq.framework.dataprotection.cryptoengine.CryptoEngine;
import io.axoniq.framework.dataprotection.internal.encryption.core.Operation;
import io.axoniq.framework.dataprotection.internal.encryption.dpd.EncryptionContext;
import io.axoniq.framework.dataprotection.internal.encryption.dpd.RoutingDPDProcessor;
import io.axoniq.framework.dataprotection.internal.model.ModelRegistry;
import io.axoniq.framework.dataprotection.internal.utils.ExceptionFactory;
import org.axonframework.conversion.Converter;

import java.util.Map;
import java.util.Set;
import javax.crypto.SecretKey;

public class EncryptionProcessor {

    private final ModelRegistry modelRegistry;
    private final RoutingDPDProcessor routingDPDProcessor;
    private final CryptoEngine cryptoEngine;

    /**
     * Creates an EncryptionProcessor using the Converter API (Axon Framework 5.x).
     * <p>
     * This is the recommended constructor for Axon Framework 5.x applications.
     *
     * @param cryptoEngine the crypto engine for key management
     * @param converter the converter for object serialization
     * @param replacementValueProvider the provider for replacement values
     * @since 5.0.0
     */
    public EncryptionProcessor(CryptoEngine cryptoEngine, Converter converter, ReplacementValueProvider replacementValueProvider) {
        this.modelRegistry = new ModelRegistry();
        this.routingDPDProcessor = new RoutingDPDProcessor(modelRegistry, cryptoEngine, converter, replacementValueProvider);
        this.cryptoEngine = cryptoEngine;
    }

    /**
     * Processes the given input object, applying encryption/decryption operations.
     * <p>
     * For mutable objects, this modifies the object in-place and returns the same instance.
     * For immutable objects (like Java records), this creates and returns a new instance with modified fields.
     *
     * @param input the object to process
     * @param operation the operation to perform (ENCRYPT, DECRYPT, or REPLACE)
     * @param keyIds map of group names to key IDs
     * @param groups set of groups to process
     * @return the processed object (same instance for mutable objects, new instance for immutable objects)
     */
    public Object process(Object input, Operation operation, Map<String, String> keyIds, Set<String> groups) {
        if(input == null) {
            return null;
        }

        EncryptionContext encryptionContext = new EncryptionContext(groups);
        for(Map.Entry<String, String> entry : keyIds.entrySet()) {
            SecretKey key;
            switch(operation) {
                case ENCRYPT:
                    key = cryptoEngine.getOrCreateKey(entry.getValue());
                    break;
                case DECRYPT:
                    key = cryptoEngine.getKey(entry.getValue());
                    break;
                case REPLACE:
                    key = null;
                    break;
                default:
                    throw ExceptionFactory.missingOperation();
            }
            encryptionContext.push(entry.getKey(), key);
        }
        return routingDPDProcessor.process(input, encryptionContext, operation);
    }

    public boolean willProcess(Object input) {
        return modelRegistry.willProcess(input);
    }

    public boolean isModifyImmutableCollections() {
        return routingDPDProcessor.isModifyImmutableCollections();
    }

    public void setModifyImmutableCollections(boolean value) {
        routingDPDProcessor.setModifyImmutableCollections(value);
    }

}
