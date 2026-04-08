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

import org.jspecify.annotations.Nullable;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.message.BinaryMessageEncoder;
import org.axonframework.conversion.ConversionException;
import org.axonframework.conversion.ContentTypeConverter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * A {@link ContentTypeConverter} implementation that converts an Avro {@link GenericRecord} into a
 * single-object-encoded {@code byte[]}.
 *
 * @author Simon Zambrovski
 * @author Jan Galinski
 * @since 4.11.0
 */
public class GenericRecordToByteArrayConverter implements ContentTypeConverter<GenericRecord, byte[]> {

    @Override
    public Class<GenericRecord> expectedSourceType() {
        return GenericRecord.class;
    }

    @Override
    public Class<byte[]> targetType() {
        return byte[].class;
    }

    @Override
    public byte @Nullable [] convert(@Nullable GenericRecord input) {
        if (input == null) {
            return null;
        }

        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            new BinaryMessageEncoder<GenericRecord>(AvroUtil.genericData, input.getSchema())
                    .encode(input, baos);
            baos.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new ConversionException("Cannot convert GenericRecord to bytes.", e);
        }
    }
}
