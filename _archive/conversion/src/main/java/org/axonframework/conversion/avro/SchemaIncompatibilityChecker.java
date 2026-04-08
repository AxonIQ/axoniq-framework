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
import org.axonframework.conversion.ConversionException;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Provides functionality for incompatibility checks.
 *
 * @author Simon Zambrovski
 * @author Jan Galinski
 * @since 4.11.0
 */
public interface SchemaIncompatibilityChecker {

    /**
     * Checks schema compatibilities and throws exception if schemas are not compatible.
     *
     * @param readerType                  The intended reader type.
     * @param readerSchema                The schema available on the reader side.
     * @param writerSchema                The schema that was used to write the data.
     * @param includeSchemasInStackTraces A flag if schemas should be included in stack traces.
     * @throws ConversionException if the schema check has not passed.
     */
    default void assertSchemaCompatibility(
            Class<?> readerType,
            Schema readerSchema,
            Schema writerSchema,
            boolean includeSchemasInStackTraces) {
        List<SchemaCompatibility.Incompatibility> incompatibilities = checkCompatibility(
                readerSchema, writerSchema
        );
        if (!incompatibilities.isEmpty()) {
            // reader and writer are incompatible
            // this is a fatal error, let provide information for the developer
            String incompatibilitiesMessage = incompatibilities
                    .stream()
                    .map(AvroUtil::incompatibilityPrinter)
                    .collect(Collectors.joining(", "));
            throw AvroUtil.createExceptionFailedToDeserialize(
                    readerType,
                    readerSchema,
                    writerSchema,
                    "[" + incompatibilitiesMessage + "]",
                    includeSchemasInStackTraces
            );
        }
    }

    /**
     * Performs compatibility check.
     *
     * @param readerSchema reader schema to check.
     * @param writerSchema writer schema to check.
     * @return list of compatibilities if any, or empty list
     */
    default List<SchemaCompatibility.Incompatibility> checkCompatibility(
            Schema readerSchema,
            Schema writerSchema
    ) {
        return AvroUtil.checkCompatibility(
                readerSchema,
                writerSchema
        );
    }
}
