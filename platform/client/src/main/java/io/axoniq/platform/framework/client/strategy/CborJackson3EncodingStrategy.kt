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

package io.axoniq.platform.framework.client.strategy

import io.netty.buffer.ByteBuf
import io.netty.buffer.ByteBufAllocator
import io.netty.buffer.CompositeByteBuf
import io.rsocket.Payload
import io.rsocket.metadata.WellKnownMimeType
import io.rsocket.util.DefaultPayload
import tools.jackson.databind.DeserializationFeature
import tools.jackson.dataformat.cbor.CBORMapper

class CborJackson3EncodingStrategy : RSocketPayloadEncodingStrategy {
    init {
        try {
            Class.forName("com.fasterxml.jackson.annotation.JsonSerializeAs")
        } catch (e: ClassNotFoundException) {
            throw IllegalStateException(
                    "Axoniq Platform requires jackson-annotations 2.21 or higher due to using Jackson 3, but an older version was found on the classpath. " +
                            "Please specifically add com.fasterxml.jackson.core:jackson-annotations:2.21 or higher to your project dependencies."
            )
        }
    }

    private val mapper = CBORMapper.builder()
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .build()

    override fun getMimeType(): WellKnownMimeType {
        return WellKnownMimeType.APPLICATION_CBOR
    }

    override fun encode(payload: Any, metadata: ByteBuf?): Payload {
        val payloadBuffer: CompositeByteBuf = ByteBufAllocator.DEFAULT.compositeBuffer()
        payloadBuffer.writeBytes(mapper.writeValueAsBytes(payload))
        return DefaultPayload.create(payloadBuffer, metadata)
    }

    override fun <T> decode(payload: Payload, expectedType: Class<T>): T {
        if (expectedType == String::class.java) {
            return payload.dataUtf8 as T
        }

        return mapper.readValue(payload.data.array(), expectedType)
    }
}
