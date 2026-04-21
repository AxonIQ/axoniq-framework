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

package io.axoniq.platform.framework.client

import io.axoniq.platform.framework.api.PlatformClientAuthentication
import io.netty.buffer.ByteBufAllocator
import io.netty.buffer.CompositeByteBuf
import io.rsocket.metadata.CompositeMetadataCodec
import io.rsocket.metadata.TaggingMetadataCodec
import io.rsocket.metadata.WellKnownMimeType


fun CompositeByteBuf.addRouteMetadata(route: String) {
    val routingMetadata = TaggingMetadataCodec.createRoutingMetadata(ByteBufAllocator.DEFAULT, listOf(route))
    CompositeMetadataCodec.encodeAndAddMetadata(
        this,
        ByteBufAllocator.DEFAULT,
        WellKnownMimeType.MESSAGE_RSOCKET_ROUTING,
        routingMetadata.content
    )
}

fun CompositeByteBuf.addAuthMetadata(auth: PlatformClientAuthentication) {
    val authMetadata = ByteBufAllocator.DEFAULT.compositeBuffer()
    authMetadata.writeBytes(auth.toBearerToken().toByteArray())
    CompositeMetadataCodec.encodeAndAddMetadata(
        this,
        ByteBufAllocator.DEFAULT,
        WellKnownMimeType.MESSAGE_RSOCKET_AUTHENTICATION,
        authMetadata
    )
}
