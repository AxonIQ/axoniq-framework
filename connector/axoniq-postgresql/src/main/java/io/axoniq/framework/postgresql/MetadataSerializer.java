/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 *
 * The software is available for evaluation use without registration.
 * Continued use beyond the evaluation period requires registration
 * and a commercial license. See the License for the specific language
 * governing permissions and limitations under the License.
 * You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at:
 *  https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *  https://www.axoniq.io/pricing
 */

package io.axoniq.framework.postgresql;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.util.Map;

/**
 * Utility for dealing with JSON metadata objects.
 *
 * @author John Hendrikx
 * @since 5.2.0
 */
class MetadataSerializer {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().configure(SerializationFeature.INDENT_OUTPUT, false);
    private static final TypeReference<Map<String, String>> STRING_TO_STRING_MAP_TYPE_REFERENCE = new TypeReference<>() {};

    static Map<String, String> fromJson(String json) {
        try {
            return OBJECT_MAPPER.readValue(json, STRING_TO_STRING_MAP_TYPE_REFERENCE);
        }
        catch (JsonProcessingException e) {  // should never occur
            throw new IllegalStateException(e);
        }
    }

    static String toJson(Map<String, String> metadata) {
        try {
            return OBJECT_MAPPER.writeValueAsString(metadata);
        }
        catch (JsonProcessingException e) {  // should never occur
            throw new IllegalStateException(e);
        }
    }

    private MetadataSerializer() {}
}
