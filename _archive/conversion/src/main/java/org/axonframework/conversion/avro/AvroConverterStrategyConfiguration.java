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

/**
 * Configuration for the Avro converter strategy.
 * @param performAvroCompatibilityCheck Should schema compatibility check be performed prio conversion.
 * @param includeSchemasInStackTraces Should Avro schemas be included into stack traces on errors.
 * @author Simon Zambrovski
 * @author Jan Galinski
 * @since 4.11.0
 */
public record AvroConverterStrategyConfiguration(
        boolean performAvroCompatibilityCheck,
        boolean includeSchemasInStackTraces
) {

    /**
     * Default configuration.
     */
    public static final AvroConverterStrategyConfiguration DEFAULT
            = new AvroConverterStrategyConfiguration(true, false);
}
