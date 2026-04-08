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


import org.apache.avro.generic.GenericRecord;
import org.apache.avro.message.SchemaStore;
import org.axonframework.conversion.ConversionException;
import org.axonframework.conversion.avro.test.ComplexObject;
import org.junit.jupiter.api.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Strategy test.
 *
 * @author Simon Zambrovski
 * @since 5.0.0
 */
class SpecificRecordBaseConverterStrategyTest {

    private final SchemaStore.Cache cache = new SchemaStore.Cache();
    private final SpecificRecordBaseConverterStrategy testSubject = new SpecificRecordBaseConverterStrategy(
            cache,
            new DefaultSchemaIncompatibilityChecker()
    );
    private final ComplexObject testComplexObject = ComplexObject
            .newBuilder()
            .setValue1("value1")
            .setValue2("value2")
            .setValue3(42)
            .build();

    @BeforeEach
    public void clean() {
        cache.addSchema(ComplexObject.getClassSchema());
    }

    @Test
    public void rejectsUnsupportedTypes() {
        assertThat(testSubject.test(Integer.class)).isFalse();

        final byte[] encodedBytes = testSubject.convertToSingleObjectEncoded(testComplexObject);
        assertEquals("Expected reader type to be assignable from SpecificRecordBase but it was java.lang.Integer",
                     assertThrows(ConversionException.class,
                                  () -> testSubject.convertFromSingleObjectEncoded(encodedBytes, Integer.class)
                     ).getMessage()
        );

        final GenericRecord record = testComplexObject;
        assertEquals("Expected reader type to be assignable from SpecificRecordBase but it was java.lang.Integer",
                     assertThrows(ConversionException.class,
                                  () -> testSubject.convertFromGenericRecord(record, Integer.class)
                     ).getMessage()
        );


        assertEquals("Expected object to be instance of SpecificRecordBase but it was java.lang.Integer",
                     assertThrows(ConversionException.class,
                                  () -> testSubject.convertToSingleObjectEncoded(42)
                     ).getMessage()
        );
    }
}