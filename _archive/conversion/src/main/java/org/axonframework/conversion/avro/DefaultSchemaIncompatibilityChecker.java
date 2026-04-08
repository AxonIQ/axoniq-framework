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
package org.axonframework.conversion.avro;

import org.apache.avro.Schema;
import org.apache.avro.SchemaCompatibility;
import org.apache.commons.lang3.tuple.Pair;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Provides functionality for incompatibility checks and saves the results in a cache.
 *
 * @author Simon Zambrovski
 * @author Jan Galinski
 * @since 4.11.0
 */
public class DefaultSchemaIncompatibilityChecker implements SchemaIncompatibilityChecker {

    private final ConcurrentHashMap<Pair<Long, Long>, List<SchemaCompatibility.Incompatibility>> cache
            = new ConcurrentHashMap<>();

    @Override
    public List<SchemaCompatibility.Incompatibility> checkCompatibility(
            Schema readerSchema,
            Schema writerSchema
    ) {
        return cache.computeIfAbsent(
                Pair.of(AvroUtil.fingerprint(readerSchema), AvroUtil.fingerprint(writerSchema)),
                (key) -> SchemaIncompatibilityChecker.super
                        .checkCompatibility(readerSchema, writerSchema)
        );
    }

    /**
     * Retrieves a list of incompatibilities cached so far.
     * Visible for testing.
     * @return copy of immutability map.
     */
    Map<Pair<Long, Long>, List<SchemaCompatibility.Incompatibility>> getIncompatibilitiesCache() {
        HashMap<Pair<Long, Long>, List<SchemaCompatibility.Incompatibility>> copy = new HashMap<>(cache.size());
        copy.putAll(cache);
        return copy;
    }

    /**
     * Clears the cache.
     */
    void clear() {
        cache.clear();
    }
}
