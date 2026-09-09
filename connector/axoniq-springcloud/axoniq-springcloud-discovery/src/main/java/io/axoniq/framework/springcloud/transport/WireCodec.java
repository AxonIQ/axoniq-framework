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

package io.axoniq.framework.springcloud.transport;

import org.axonframework.common.annotation.Internal;
import org.axonframework.conversion.ConversionException;
import org.axonframework.conversion.Converter;
import org.axonframework.messaging.core.HandlerExecutionException;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Base64;
import java.util.List;

/**
 * The encoding members share when writing a failure to each other, whatever kind of message it answers.
 * <p>
 * Messages of different kinds travel over endpoints of their own, with shapes of their own, but a failure is
 * described the same way in all of them. Keeping that here means those shapes cannot drift apart into encodings that
 * no longer read each other.
 *
 * @author Allard Buijze
 * @since 5.4.0
 */
@Internal
final class WireCodec {

    private static final Logger logger = LoggerFactory.getLogger(WireCodec.class);

    private static final int MAX_ERROR_DESCRIPTIONS = 10;

    private WireCodec() {
        // Utility class, not meant to be instantiated.
    }

    /**
     * Encodes the given {@code bytes} for transport, as {@code null} when there is nothing to carry.
     *
     * @param bytes the bytes to encode
     * @return the encoded form of the given {@code bytes}, or {@code null} when there are none
     */
    static @Nullable String encode(byte @Nullable [] bytes) {
        return bytes == null || bytes.length == 0 ? null : Base64.getEncoder().encodeToString(bytes);
    }

    /**
     * Decodes what {@link #encode(byte[])} wrote.
     *
     * @param encoded the encoded bytes, or {@code null} when none were carried
     * @return the bytes the given {@code encoded} form represents, or {@code null} when it is {@code null}
     */
    static byte @Nullable [] decode(@Nullable String encoded) {
        return encoded == null ? null : Base64.getDecoder().decode(encoded);
    }

    /**
     * Describes the given {@code cause}, falling back to its type when it carries no message.
     *
     * @param cause the failure to describe
     * @return a description of the given {@code cause}
     */
    static String messageOf(Throwable cause) {
        return cause.getMessage() == null ? cause.getClass().getName() : cause.getMessage();
    }

    /**
     * Describes the given {@code cause} and the causes behind it, outermost first.
     * <p>
     * The chain is followed to a bounded depth, so that a cycle or a pathologically deep chain cannot turn a failure
     * into an unbounded reply.
     *
     * @param cause the failure to describe
     * @return descriptions of the given {@code cause} and the causes behind it
     */
    static List<String> descriptionsOf(Throwable cause) {
        List<String> descriptions = new ArrayList<>();
        Throwable current = cause;
        while (current != null && descriptions.size() < MAX_ERROR_DESCRIPTIONS) {
            descriptions.add(messageOf(current));
            current = current.getCause() == current ? null : current.getCause();
        }
        return descriptions;
    }

    /**
     * Returns the application-specific details the given {@code cause} carries, as the bytes to write.
     * <p>
     * Details that cannot be converted are omitted rather than raised: a failure that cannot be described in full is
     * still worth reporting, and replacing it with a conversion failure would hide what actually went wrong.
     *
     * @param cause     the failure whose details to write
     * @param converter the converter to write the details with, or {@code null} when none is available
     * @return the bytes to write for the given {@code cause}'s details, or {@code null} when it carries none that can
     * be written
     */
    static byte @Nullable [] serializedDetailsOf(Throwable cause, @Nullable Converter converter) {
        Object details = HandlerExecutionException.resolveDetails(cause).orElse(null);
        if (details == null) {
            return null;
        }
        if (details instanceof byte[] rawDetails) {
            return rawDetails;
        }
        if (converter == null) {
            logger.debug("Cannot convert exception details of type [{}] as no Converter is available; omitting them.",
                         details.getClass().getName());
            return null;
        }
        try {
            return converter.convert(details, byte[].class);
        } catch (ConversionException e) {
            logger.debug("Could not serialize exception details of type [{}]; omitting them from the reply.",
                         details.getClass().getName(), e);
            return null;
        }
    }
}
