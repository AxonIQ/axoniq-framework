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

package org.axonframework.conversion.json;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.jsontype.TypeDeserializer;
import org.axonframework.messaging.core.Metadata;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * JsonDeserializer implementation for Jackson 2 that deserializes Metadata instances.
 *
 * @author Allard Buijze
 * @since 2.4.2
 * @deprecated in favor of the {@link JacksonConverter}.
 * TODO #3602 remove
 */
@Deprecated(forRemoval = true, since = "5.0.0")
public class MetadataDeserializer extends JsonDeserializer<Metadata> {

    @Override
    public Object deserializeWithType(JsonParser jsonParser, DeserializationContext ctxt, TypeDeserializer typeDeserializer) throws IOException {
        return typeDeserializer.deserializeTypedFromObject(jsonParser, ctxt);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Metadata deserialize(JsonParser jp, DeserializationContext ctxt)
            throws IOException {
        JsonDeserializer<Object> deserializer = ctxt.findRootValueDeserializer(
                ctxt.getTypeFactory().constructMapType(Map.class, String.class, Object.class));

        return Metadata.from((Map) deserializer.deserialize(jp, ctxt, new HashMap<>()));
    }
}
