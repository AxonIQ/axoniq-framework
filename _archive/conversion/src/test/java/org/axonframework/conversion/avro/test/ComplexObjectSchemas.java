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

package org.axonframework.conversion.avro.test;

import org.apache.avro.Schema;

/**
 * Collection of (slightly) incompatible schemas used for testing.
 *
 * @author Simon Zambrovski
 * @author Jan Galinski
 * @since 4.11.0
 */
public final class ComplexObjectSchemas {

    public static final Schema compatibleSchemaWithoutValue2 = parse(
            "{\n" +
                    "  \"name\": \"ComplexObject\",\n" +
                    "  \"namespace\": \"org.axonframework.conversion.avro.test\",\n" +
                    "  \"type\": \"record\",\n" +
                    "  \"fields\": [\n" +
                    "    {\n" +
                    "      \"name\": \"value1\",\n" +
                    "      \"type\": \"string\"\n" +
                    "    },\n" +
                    "    {\n" +
                    "      \"name\": \"value3\",\n" +
                    "      \"type\": \"int\"\n" +
                    "    }\n" +
                    "  ]\n" +
                    "}");

    public static final Schema incompatibleSchema = parse("{\n" +
            "  \"name\": \"ComplexObject\",\n" +
            "  \"namespace\": \"org.axonframework.conversion.avro.test\",\n" +
            "  \"type\": \"record\",\n" +
            "  \"fields\": [\n" +
            "    {\n" +
            "      \"name\": \"value2\",\n" +
            "      \"type\": \"string\"\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"value3\",\n" +
            "      \"type\": \"int\"\n" +
            "    }\n" +
            "  ]\n" +
            "}");
    public static final Schema compatibleSchema = parse("{\n" +
            "  \"name\": \"ComplexObject\",\n" +
            "  \"namespace\": \"org.axonframework.conversion.avro.test\",\n" +
            "  \"type\": \"record\",\n" +
            "  \"fields\": [\n" +
            "    {\n" +
            "      \"name\": \"value1\",\n" +
            "      \"type\": \"string\"\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"value2\",\n" +
            "      \"type\": \"string\"\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"value3\",\n" +
            "      \"type\": \"int\"\n" +
            "    }\n" +
            "  ]\n" +
            "}");
    public static final Schema compatibleSchemaWithAdditionalField = parse("{\n" +
            "  \"name\": \"ComplexObject\",\n" +
            "  \"namespace\": \"org.axonframework.conversion.avro.test\",\n" +
            "  \"type\": \"record\",\n" +
            "  \"fields\": [\n" +
            "    {\n" +
            "      \"name\": \"value1\",\n" +
            "      \"type\": \"string\"\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"value2\",\n" +
            "      \"type\": \"string\"\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"value4\",\n" +
            "      \"type\": \"string\"\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"value3\",\n" +
            "      \"type\": \"int\"\n" +
            "    }\n" +
            "  ]\n" +
            "}");

    static Schema parse(String json) {
        return new Schema.Parser().parse(json);
    }

    private ComplexObjectSchemas() {
    }
}
