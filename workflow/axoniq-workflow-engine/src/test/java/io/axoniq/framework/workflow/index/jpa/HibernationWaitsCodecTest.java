/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.index.jpa;

import io.axoniq.framework.workflow.index.api.WorkflowIndex.OpenWait;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests the JSON text {@link HibernationWaitsCodec} writes for the open waits of a hibernated workflow, and reads back.
 *
 * @author Allard Buijze
 */
class HibernationWaitsCodecTest {

    @Nested
    class Encode {

        @Test
        void writesReadableJson() {
            // given
            var waits = List.of(new OpenWait("awaitPayment", "com.acme.PaymentReceived", Set.of("payload:orderId=42")));

            // when
            var json = HibernationWaitsCodec.encode(waits);

            // then
            assertThat(json).isEqualTo("[{\"step\":\"awaitPayment\",\"event\":\"com.acme.PaymentReceived\","
                                               + "\"associations\":[\"payload:orderId=42\"]}]");
        }

        @Test
        void writesAWaitWithoutEventNameAsNullAndNoWaitsAsAnEmptyArray() {
            // when
            var withoutEvent = HibernationWaitsCodec.encode(List.of(new OpenWait("any", null, Set.of())));
            var none = HibernationWaitsCodec.encode(List.of());

            // then
            assertThat(withoutEvent).isEqualTo("[{\"step\":\"any\",\"event\":null,\"associations\":[]}]");
            assertThat(none).isEqualTo("[]");
        }

        @Test
        void escapesQuotesBackslashesAndControlCharacters() {
            // when
            var json = HibernationWaitsCodec.encode(List.of(new OpenWait("a\"b\\c\nd\u0001", "e\u007f", Set.of())));

            // then
            assertThat(json).isEqualTo(
                    "[{\"step\":\"a\\\"b\\\\c\\nd\\u0001\",\"event\":\"e\\u007f\",\"associations\":[]}]"
            );
        }

        @Test
        void escapesEveryCharacterOutsidePrintableAsciiIncludingLoneSurrogates() {
            // given - non-ASCII text, a supplementary character, and lone surrogates, which are no valid UTF-8 as is
            var waits = List.of(new OpenWait("caf\u00e9", "\ud83d\ude00", Set.of("high:\ud800", "low:\udc00x")));

            // when
            var json = HibernationWaitsCodec.encode(waits);

            // then - the text is printable ASCII, so any column encoding stores it unchanged
            assertThat(json).isEqualTo("[{\"step\":\"caf\\u00e9\",\"event\":\"\\ud83d\\ude00\","
                                               + "\"associations\":[\"high:\\ud800\",\"low:\\udc00x\"]}]");
            assertThat(json.chars()).allMatch(c -> c >= 0x20 && c < 0x7f);
        }
    }

    @Nested
    class RoundTrip {

        @Test
        void readsBackWhatItWrote() {
            // given - texts that need escaping, non-ASCII text, a wait without event name and without associations
            var waits = List.of(
                    new OpenWait("await \"payment\"",
                                 "com.acme.Payment\\Received",
                                 Set.of("payload:orderId=42", "metadata:note=caf\u00e9 \u4e2d\u6587 \ud83d\ude00",
                                        "payload:text=line\nbreak\ttab\r\b\f\u001f/")),
                    new OpenWait("anyEvent", null, Set.of())
            );

            // when
            var read = HibernationWaitsCodec.decode(HibernationWaitsCodec.encode(waits));

            // then
            assertThat(read).isEqualTo(waits);
        }

        @Test
        void readsBackLoneSurrogatesAndSupplementaryCharacters() {
            // given
            var waits = List.of(new OpenWait("\ud800", "\udfff\ud800", Set.of("pair:\ud83d\ude00", "high-at-end:\udbff",
                                                                           "low-first:\udc00\ud83d", "\uffff\u0080")));

            // when
            var read = HibernationWaitsCodec.decode(HibernationWaitsCodec.encode(waits));

            // then
            assertThat(read).isEqualTo(waits);
        }

        @Test
        void readsBackNoWaits() {
            // when
            var read = HibernationWaitsCodec.decode(HibernationWaitsCodec.encode(List.of()));

            // then
            assertThat(read).isEmpty();
        }
    }

    @Nested
    class Decode {

        @Test
        void acceptsWhitespaceEscapedUnicodeAndMembersItDoesNotKnow() {
            // given - written by hand, or by a later version that adds members
            var json = """
                    [ { "step" : "await\\u0050ayment", "event" : "com.acme\\/Paid",
                        "associations" : [ "payload:id=1" ], "since" : 1.5e3, "strict" : true,
                        "extra" : { "nested" : [ null, false ] } } ]
                    """;

            // when
            var read = HibernationWaitsCodec.decode(json);

            // then
            assertThat(read).containsExactly(new OpenWait("awaitPayment", "com.acme/Paid", Set.of("payload:id=1")));
        }

        @Test
        void readsAWaitWithoutAssociationsMember() {
            // when
            var read = HibernationWaitsCodec.decode("[{\"step\":\"s\",\"event\":null}]");

            // then
            assertThat(read).containsExactly(new OpenWait("s", null, Set.of()));
        }

        @Test
        void rejectsTextThatIsNoArrayOfWaits() {
            assertThatIllegalArgumentException().isThrownBy(() -> HibernationWaitsCodec.decode("{}"));
            assertThatIllegalArgumentException().isThrownBy(() -> HibernationWaitsCodec.decode("[1]"));
            assertThatIllegalArgumentException().isThrownBy(() -> HibernationWaitsCodec.decode("[{\"event\":null}]"));
            assertThatIllegalArgumentException().isThrownBy(
                    () -> HibernationWaitsCodec.decode("[{\"step\":\"s\",\"event\":1}]")
            );
            assertThatIllegalArgumentException().isThrownBy(
                    () -> HibernationWaitsCodec.decode("[{\"step\":\"s\",\"associations\":[1]}]")
            );
        }

        @Test
        void rejectsMalformedJson() {
            assertThatIllegalArgumentException().isThrownBy(() -> HibernationWaitsCodec.decode(""));
            assertThatIllegalArgumentException().isThrownBy(() -> HibernationWaitsCodec.decode("[{\"step\":"));
            assertThatIllegalArgumentException().isThrownBy(() -> HibernationWaitsCodec.decode("[\"unterminated]"));
            assertThatIllegalArgumentException().isThrownBy(() -> HibernationWaitsCodec.decode("[\"\\x\"]"));
            assertThatIllegalArgumentException().isThrownBy(() -> HibernationWaitsCodec.decode("[\"\\u12\"]"));
            assertThatIllegalArgumentException().isThrownBy(() -> HibernationWaitsCodec.decode("[] trailing"));
        }

        @Test
        void rejectsAUnicodeEscapeWithoutFourHexDigits() {
            // when / then - four hex digits are read, anything else is rejected
            assertThat(HibernationWaitsCodec.decode("[{\"step\":\"\\u0123\"}]"))
                    .containsExactly(new OpenWait("\u0123", null, Set.of()));
            assertThatIllegalArgumentException()
                    .isThrownBy(() -> HibernationWaitsCodec.decode("[{\"step\":\"\\u+123\"}]"));
            assertThatIllegalArgumentException()
                    .isThrownBy(() -> HibernationWaitsCodec.decode("[{\"step\":\"\\u-123\"}]"));
            assertThatIllegalArgumentException()
                    .isThrownBy(() -> HibernationWaitsCodec.decode("[{\"step\":\"\\u12g4\"}]"));
            assertThatIllegalArgumentException()
                    .isThrownBy(() -> HibernationWaitsCodec.decode("[{\"step\":\"\\u\"}]"));
            // a digit that is no ASCII hex digit, such as a fullwidth one
            assertThatIllegalArgumentException()
                    .isThrownBy(() -> HibernationWaitsCodec.decode("[{\"step\":\"\\u012\uff13\"}]"));
        }

        @Test
        void rejectsDeeplyNestedJsonWithoutOverflowingTheStack() {
            // given - a corrupt column of nested arrays and objects
            var arrays = "[".repeat(100_000);
            var objects = "[" + "{\"a\":".repeat(100_000);

            // when / then
            assertThatIllegalArgumentException().isThrownBy(() -> HibernationWaitsCodec.decode(arrays));
            assertThatIllegalArgumentException().isThrownBy(() -> HibernationWaitsCodec.decode(objects));
        }
    }
}
