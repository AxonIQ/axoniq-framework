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
package io.axoniq.framework.extension.dataprotection.cryptoengine;

import java.util.concurrent.ConcurrentHashMap;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

/**
 * Implementation of {@link CryptoEngine} that extends from {@link DatabaseBackedCryptoEngine} and simply
 * keeps all its data in an in-memory {@link ConcurrentHashMap}. This is useful for (unit) testing purposes.
 *
 * @author Frans van Buul
 */
public class InMemoryCryptoEngine extends DatabaseBackedCryptoEngine {

    private static final ConcurrentHashMap<String, SecretKeySpec> db = new ConcurrentHashMap<>();

     /**
      * Constructs an {@link InMemoryCryptoEngine}. There are no parameters to configure.
      */
     public InMemoryCryptoEngine() {
     }

    @Override
    public SecretKey getKey(String id) {
        return db.get(id);
    }

    @Override
    protected SecretKey putKeyIfAbsent(String id, SecretKeySpec secretKeySpec) {
        SecretKey previous = db.putIfAbsent(id, secretKeySpec);
        return previous == null ? secretKeySpec : previous;
    }

    @Override
    public void deleteKey(String id) {
        db.remove(id);
    }
}
