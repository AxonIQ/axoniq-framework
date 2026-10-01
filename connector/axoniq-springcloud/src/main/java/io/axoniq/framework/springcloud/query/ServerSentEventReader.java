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

package io.axoniq.framework.springcloud.query;

import org.axonframework.common.annotation.Internal;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Objects;
import java.util.function.Consumer;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Reads a Server-Sent Events stream, handing each complete event to a consumer as it arrives.
 * <p>
 * Members stream query responses to each other as Server-Sent Events. This reader is what makes that possible without
 * a reactive stack: it consumes the response body directly, so the module needs no client that parses the protocol on
 * its behalf, and thus neither WebFlux nor Reactor.
 * <p>
 * Reading blocks until the stream ends, so callers run it on a thread of their own. Closing the {@code body} from
 * another thread is how a caller stops a read it no longer needs; the blocked read then fails, which the caller
 * recognises as its own cancellation.
 * <p>
 * The {@code retry} field is read and discarded: when to reconnect is a policy of the member doing the reading, not
 * of the member it is reading from.
 *
 * @author Allard Buijze
 * @since 5.4.0
 */
@Internal
final class ServerSentEventReader {

    private static final char COMMENT_PREFIX = ':';
    private static final char FIELD_SEPARATOR = ':';
    private static final char BOM = '\uFEFF';

    private ServerSentEventReader() {
        // Utility class, not meant to be instantiated.
    }

    /**
     * Reads events from the given {@code body}, handing each complete event to {@code onEvent}, until the stream ends.
     * <p>
     * An event is complete once a blank line closes it, which is also when {@code onEvent} sees it. An event still
     * being accumulated when the stream ends is discarded, as the protocol prescribes: a partially received event is
     * not a shorter event.
     *
     * @param body    the response body to read events from
     * @param onEvent receives every complete event, on the calling thread
     * @throws IOException when reading the {@code body} fails, which includes the {@code body} being closed to stop
     *                     the read
     */
    public static void read(InputStream body, Consumer<ServerSentEvent> onEvent) throws IOException {
        Objects.requireNonNull(body, "The body must not be null.");
        Objects.requireNonNull(onEvent, "The onEvent consumer must not be null.");

        BufferedReader reader = new BufferedReader(new InputStreamReader(body, UTF_8));
        StringBuilder data = new StringBuilder();
        String eventType = ServerSentEvent.DEFAULT_EVENT_TYPE;
        String lastId = null;
        boolean firstLine = true;

        String line;
        while ((line = reader.readLine()) != null) {
            if (firstLine) {
                // A stream may open with a byte order mark, which is not part of the first field's name.
                line = stripByteOrderMark(line);
                firstLine = false;
            }
            if (line.isEmpty()) {
                // A blank line closes the event. Without data there is no event to report, only a type to forget.
                if (data.isEmpty()) {
                    eventType = ServerSentEvent.DEFAULT_EVENT_TYPE;
                    continue;
                }
                data.setLength(data.length() - 1); // The separator the last data line added.
                onEvent.accept(new ServerSentEvent(lastId, eventType, data.toString()));
                data.setLength(0);
                eventType = ServerSentEvent.DEFAULT_EVENT_TYPE;
                continue;
            }
            if (line.charAt(0) == COMMENT_PREFIX) {
                // A comment, which is how a member keeps an otherwise idle stream alive.
                continue;
            }

            int separator = line.indexOf(FIELD_SEPARATOR);
            String field = separator < 0 ? line : line.substring(0, separator);
            String value = separator < 0 ? "" : stripLeadingSpace(line.substring(separator + 1));
            switch (field) {
                case "event" -> eventType = value;
                case "data" -> data.append(value).append('\n');
                case "id" -> {
                    // An identifier containing a NUL is ignored rather than remembered, so that reporting it back on
                    // a reconnect cannot ask the serving member to resume from something it never sent.
                    if (value.indexOf('\0') < 0) {
                        lastId = value;
                    }
                }
                default -> {
                    // Any other field, "retry" included, carries nothing this reader acts on.
                }
            }
        }
    }

    private static String stripByteOrderMark(String line) {
        return !line.isEmpty() && line.charAt(0) == BOM ? line.substring(1) : line;
    }

    private static String stripLeadingSpace(String value) {
        return !value.isEmpty() && value.charAt(0) == ' ' ? value.substring(1) : value;
    }
}
