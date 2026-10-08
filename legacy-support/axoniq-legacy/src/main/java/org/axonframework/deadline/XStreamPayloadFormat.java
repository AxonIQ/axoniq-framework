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

package org.axonframework.deadline;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.XStreamException;
import com.thoughtworks.xstream.io.xml.CompactWriter;
import com.thoughtworks.xstream.security.NoTypePermission;
import org.jspecify.annotations.Nullable;

import java.io.StringWriter;

/**
 * Writes and reads a {@code String} or {@code byte[]} deadline payload as the XStream {@code <string>} or
 * {@code <byte-array>} element that Axon Framework 4's {@code XStreamSerializer} stored, through XStream itself, so
 * that the escaping and encoding match what Axon Framework 4 wrote and read.
 * <p>
 * Only {@link StoredDeadlineConverter} uses this class. It is kept apart so that XStream, an optional dependency, is
 * only loaded when a deadline is stored or read in the XStream format. Its {@link XStream} instance allows
 * {@code String} and {@code byte[]} only, so stored XML naming any other type is rejected instead of instantiated.
 *
 * @author Jakob Hatzl
 * @since 5.4.0
 */
final class XStreamPayloadFormat {

    private static final XStream XSTREAM = createXStream();

    private XStreamPayloadFormat() {
        // Utility class
    }

    /**
     * Writes the given {@code payload} as the XStream element Axon Framework 4 stored it as, with XStream's
     * {@link CompactWriter}, the writer Axon Framework 4's {@code XStreamSerializer} used.
     *
     * @param payload the {@code String} or {@code byte[]} payload to write
     * @return the {@code <string>} or {@code <byte-array>} element holding the given {@code payload}
     */
    static String write(Object payload) {
        StringWriter xml = new StringWriter();
        XSTREAM.marshal(payload, new CompactWriter(xml));
        return xml.toString();
    }

    /**
     * Reads the given stored {@code xml}, a {@code <string>} or {@code <byte-array>} element.
     *
     * @param xml the stored XStream element
     * @return the {@code String} or {@code byte[]} the element holds, or {@code null} if it holds none
     * @throws DeadlineException if the given {@code xml} is no {@code <string>} or {@code <byte-array>} element
     */
    static @Nullable Object read(String xml) {
        try {
            return XSTREAM.fromXML(xml);
        } catch (XStreamException e) {
            throw new DeadlineException("The stored payload is no XStream <string> or <byte-array> element", e);
        }
    }

    private static XStream createXStream() {
        XStream xStream = new XStream();
        xStream.addPermission(NoTypePermission.NONE);
        xStream.allowTypes(new Class[]{String.class, byte[].class});
        return xStream;
    }
}
