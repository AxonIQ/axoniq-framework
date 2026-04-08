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

import org.apache.avro.message.SchemaStore;
import org.axonframework.conversion.avro.AvroConverter;
import org.axonframework.conversion.avro.AvroConverterConfiguration;
import org.axonframework.conversion.avro.DefaultSchemaIncompatibilityChecker;
import org.axonframework.conversion.avro.SpecificRecordBaseConverterStrategy;
import org.junit.jupiter.api.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Test for the avro converter configuration.
 *
 * @author Simon Zambrovski
 * @since 5.0.0
 */
class AvroConverterConfigurationTest {

    @Test
    void buildsConverterFromConfigOverrideFlippingAllValues() {
        var store = new SchemaStore.Cache();
        var schemaIncompatibilitiesChecker = new DefaultSchemaIncompatibilityChecker();
        var converter = new AvroConverter(
                store,
                (c) -> c
                        .addConverterStrategy(
                                new SpecificRecordBaseConverterStrategy(store, schemaIncompatibilitiesChecker)
                        )
                        .includeDefaultAvroConverterStrategies(false)
                        .includeSchemasInStackTraces(true)
                        .performAvroCompatibilityCheck(false)
                        .includeSchemasInStackTraces(true)
                        .schemaIncompatibilityChecker(schemaIncompatibilitiesChecker)
        );
        assertThat(converter).isNotNull();
    }

    @Test
    void configurationMandatoryValues() {
        assertEquals("Schema store cannot be null",
                     assertThrows(NullPointerException.class,
                                  () -> new AvroConverterConfiguration(null)
                     )
                             .getMessage()
        );

        // that should work fine
        assertNotNull(
                new AvroConverterConfiguration(new SchemaStore.Cache())
                        .addConverterStrategy(new SpecificRecordBaseConverterStrategy(
                                new SchemaStore.Cache(),
                                new DefaultSchemaIncompatibilityChecker()
                        ))
                        .includeDefaultAvroConverterStrategies(false)
        );
    }

    @SuppressWarnings("DataFlowIssue")
    @Test
    void builderSetterContracts() {

        assertEquals("At least one Avro converter strategy is required and no default strategies will be used",
                     assertThrows(IllegalArgumentException.class,
                                  () -> new AvroConverterConfiguration(new SchemaStore.Cache())
                                          .includeDefaultAvroConverterStrategies(false)
                     ).getMessage()
        );

        assertEquals("Avro converter strategy cannot be null",
                     assertThrows(NullPointerException.class,
                                  () -> new AvroConverterConfiguration(new SchemaStore.Cache())
                                          .addConverterStrategy(
                                                  null))
                             .getMessage()
        );

        assertEquals("Schema incompatibility checker cannot be null",
                     assertThrows(NullPointerException.class,
                                  () -> new AvroConverterConfiguration(new SchemaStore.Cache())
                                          .schemaIncompatibilityChecker(
                                                  null))
                             .getMessage()
        );
    }
}
