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

import org.junit.jupiter.api.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests how {@link ServerSentEventReader} turns a Server-Sent Events stream into complete events.
 *
 * @author Allard Buijze
 */
class ServerSentEventReaderTest {

    private final List<ServerSentEvent> received = new ArrayList<>();

    private void read(String stream) throws IOException {
        ServerSentEventReader.read(new ByteArrayInputStream(stream.getBytes(UTF_8)), received::add);
    }

    @Nested
    class ReadingEvents {

        @Test
        void reportsADataOnlyEventUnderTheDefaultType() throws IOException {
            // given
            String stream = "data: first\n\n";

            // when
            read(stream);

            // then
            assertThat(received).containsExactly(new ServerSentEvent(null, "message", "first"));
        }

        @Test
        void reportsTheEventTypeTheStreamNames() throws IOException {
            // given
            String stream = "event: response\ndata: first\n\n";

            // when
            read(stream);

            // then
            assertThat(received).containsExactly(new ServerSentEvent(null, "response", "first"));
        }

        @Test
        void joinsMultipleDataLinesWithNewlines() throws IOException {
            // given a payload whose own formatting spans lines
            String stream = "data: {\ndata:   \"a\": 1\ndata: }\n\n";

            // when
            read(stream);

            // then
            assertThat(received).singleElement()
                                .extracting(ServerSentEvent::data)
                                .isEqualTo("{\n  \"a\": 1\n}");
        }

        @Test
        void readsSuccessiveEventsFromOneStream() throws IOException {
            // given
            String stream = "data: first\n\ndata: second\n\n";

            // when
            read(stream);

            // then
            assertThat(received).extracting(ServerSentEvent::data).containsExactly("first", "second");
        }

        @Test
        void stripsOnlyTheSingleSpaceAfterTheFieldSeparator() throws IOException {
            // given a payload that starts with whitespace of its own
            String stream = "data:  indented\n\n";

            // when
            read(stream);

            // then
            assertThat(received).singleElement().extracting(ServerSentEvent::data).isEqualTo(" indented");
        }

        @Test
        void readsAFieldWithoutASeparatorAsAnEmptyValue() throws IOException {
            // given
            String stream = "data\ndata: second line\n\n";

            // when
            read(stream);

            // then
            assertThat(received).singleElement().extracting(ServerSentEvent::data).isEqualTo("\nsecond line");
        }

        @Test
        void readsTheSameEventsRegardlessOfWhereTheStreamIsChopped() throws IOException {
            // given a stream that arrives one byte at a time, as a slow network delivers one
            String stream = "event: response\ndata: first\n\ndata: second\n\n";

            // when
            ServerSentEventReader.read(new DribblingInputStream(stream), received::add);

            // then
            assertThat(received).containsExactly(new ServerSentEvent(null, "response", "first"),
                                                 new ServerSentEvent(null, "message", "second"));
        }
    }

    @Nested
    class HandlingLineEndings {

        @Test
        void readsCarriageReturnLineFeedEndings() throws IOException {
            // given
            String stream = "data: first\r\n\r\ndata: second\r\n\r\n";

            // when
            read(stream);

            // then
            assertThat(received).extracting(ServerSentEvent::data).containsExactly("first", "second");
        }

        @Test
        void readsCarriageReturnOnlyEndings() throws IOException {
            // given
            String stream = "data: first\r\rdata: second\r\r";

            // when
            read(stream);

            // then
            assertThat(received).extracting(ServerSentEvent::data).containsExactly("first", "second");
        }

        @Test
        void ignoresAByteOrderMarkOpeningTheStream() throws IOException {
            // given a stream opening with a byte order mark
            char byteOrderMark = (char) 0xFEFF;
            String stream = byteOrderMark + "data: first\n\n";

            // when
            read(stream);

            // then the mark is not read as part of the first field's name
            assertThat(received).singleElement().extracting(ServerSentEvent::data).isEqualTo("first");
        }
    }

    @Nested
    class CarryingIdentifiers {

        @Test
        void reportsTheIdentifierTheEventCarries() throws IOException {
            // given
            String stream = "id: 1\ndata: first\n\n";

            // when
            read(stream);

            // then
            assertThat(received).singleElement().extracting(ServerSentEvent::id).isEqualTo("1");
        }

        @Test
        void carriesTheLastIdentifierSeenIntoLaterEvents() throws IOException {
            // given a stream that identifies its first event only
            String stream = "id: 1\ndata: first\n\ndata: second\n\n";

            // when
            read(stream);

            // then the second event reports the position a reconnect would resume from
            assertThat(received).extracting(ServerSentEvent::id).containsExactly("1", "1");
        }

        @Test
        void keepsThePreviousIdentifierWhenOneContainsANulCharacter() throws IOException {
            // given a second identifier that cannot be reported back on a reconnect
            char nul = (char) 0;
            String stream = "id: 1\ndata: first\n\nid: 2" + nul + "3\ndata: second\n\n";

            // when
            read(stream);

            // then the unusable identifier is ignored rather than remembered
            assertThat(received).extracting(ServerSentEvent::id).containsExactly("1", "1");
        }
    }

    @Nested
    class IgnoringWhatCarriesNoEvent {

        @Test
        void ignoresCommentsKeepingTheStreamAlive() throws IOException {
            // given a stream whose only traffic between events is a heartbeat
            String stream = ": heartbeat\ndata: first\n\n: heartbeat\n\n: heartbeat\ndata: second\n\n";

            // when
            read(stream);

            // then
            assertThat(received).extracting(ServerSentEvent::data).containsExactly("first", "second");
        }

        @Test
        void reportsNoEventForABlankLineWithoutData() throws IOException {
            // given
            String stream = "\n\n\n";

            // when
            read(stream);

            // then
            assertThat(received).isEmpty();
        }

        @Test
        void forgetsAnEventTypeThatNoDataFollowed() throws IOException {
            // given a named event that carried nothing, ahead of an unnamed one
            String stream = "event: response\n\ndata: first\n\n";

            // when
            read(stream);

            // then the abandoned type does not leak into the next event
            assertThat(received).containsExactly(new ServerSentEvent(null, "message", "first"));
        }

        @Test
        void discardsAnEventTheStreamEndedInTheMiddleOf() throws IOException {
            // given a stream cut off before the blank line closing its second event
            String stream = "data: first\n\ndata: sec";

            // when
            read(stream);

            // then a partially received event is not reported as a shorter one
            assertThat(received).extracting(ServerSentEvent::data).containsExactly("first");
        }

        @Test
        void ignoresFieldsItDoesNotActOn() throws IOException {
            // given a stream asking for a reconnection delay, which is the reading member's own policy
            String stream = "retry: 5000\nsomething: else\ndata: first\n\n";

            // when
            read(stream);

            // then
            assertThat(received).containsExactly(new ServerSentEvent(null, "message", "first"));
        }
    }

    @Nested
    class RejectingBadArguments {

        @Test
        void rejectsAMissingBody() {
            assertThatThrownBy(() -> ServerSentEventReader.read(null, received::add))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("body");
        }

        @Test
        void rejectsAMissingConsumer() {
            assertThatThrownBy(() -> ServerSentEventReader.read(InputStream.nullInputStream(), null))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("onEvent");
        }
    }

    /**
     * An {@link InputStream} handing out one byte per read, so that a reader cannot rely on a line arriving whole.
     */
    private static class DribblingInputStream extends InputStream {

        private final byte[] bytes;
        private int position;

        private DribblingInputStream(String content) {
            this.bytes = content.getBytes(UTF_8);
        }

        @Override
        public int read() {
            return position < bytes.length ? bytes[position++] & 0xFF : -1;
        }

        @Override
        public int read(byte[] target, int offset, int length) {
            if (position >= bytes.length) {
                return -1;
            }
            target[offset] = bytes[position++];
            return 1;
        }
    }
}
