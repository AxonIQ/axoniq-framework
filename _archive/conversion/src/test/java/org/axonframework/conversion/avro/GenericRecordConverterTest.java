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

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.message.SchemaStore;
import org.axonframework.conversion.ContentTypeConverter;
import org.axonframework.conversion.avro.test.ComplexObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Verifies correct behavior of {@link GenericRecord} {@link ContentTypeConverter}s
 * in isolation.
 *
 * @author Simon Zambrovski
 * @author Jan Galinski
 * @since 4.11.0
 */
public class GenericRecordConverterTest {

    private final SchemaStore.Cache schemaStore = new SchemaStore.Cache();
    private final GenericRecordToByteArrayConverter toByteArrayConverter = new GenericRecordToByteArrayConverter();
    private final ByteArrayToGenericRecordConverter toGenericRecordConverter = new ByteArrayToGenericRecordConverter(schemaStore);

    @BeforeEach
    void setUp() {
        schemaStore.addSchema(ComplexObject.getClassSchema());
    }

    @Test
    void convertBackAndForth() throws IOException {
        final GenericData.Record record = new GenericData.Record(ComplexObject.getClassSchema());
        record.put("value1", "foo");
        record.put("value2", "bar");
        record.put("value3", 4711);

        byte[] singleObjectEncodedBytes = toByteArrayConverter.convert(record);

        final GenericRecord convertedRecord = toGenericRecordConverter.convert(singleObjectEncodedBytes);
        assertEquals(record, convertedRecord);
    }
}
