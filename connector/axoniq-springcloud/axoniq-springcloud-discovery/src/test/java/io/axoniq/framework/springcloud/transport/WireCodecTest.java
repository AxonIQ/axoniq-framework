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

import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.conversion.ConversionException;
import org.axonframework.conversion.Converter;
import org.axonframework.messaging.queryhandling.QueryExecutionException;
import org.junit.jupiter.api.*;

import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests the encoding members share when writing messages to each other, whichever kind of message they are.
 *
 * @author Allard Buijze
 */
class WireCodecTest {

    @Nested
    class CarryingAPayload {

        @Test
        void carriesBytesAsTheyAre() {
            // given
            byte[] payload = "{\"id\":\"course-1\"}".getBytes(StandardCharsets.UTF_8);

            // when
            String encoded = WireCodec.encode(WireCodec.payloadAsBytes(payload, byte[].class, "Decorator"));

            // then
            assertThat(WireCodec.decode(encoded)).isEqualTo(payload);
        }

        @Test
        void carriesNothingForAMessageWithoutAPayload() {
            // when
            byte[] bytes = WireCodec.payloadAsBytes(null, Void.class, "Decorator");

            // then an absent payload travels as an absent field rather than as an empty string
            assertThat(bytes).isEmpty();
            assertThat(WireCodec.encode(bytes)).isNull();
            assertThat(WireCodec.decode(null)).isNull();
        }

        @Test
        void refusesAPayloadThatWasNeverConverted() {
            // when / then the failure names both the type it got and the decorator that should have converted it
            assertThatThrownBy(() -> WireCodec.payloadAsBytes("not bytes", String.class, "SomeDecorator"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("java.lang.String")
                    .hasMessageContaining("SomeDecorator");
        }
    }

    @Nested
    class CopyingMetadata {

        @Test
        void keepsAnEntryWithoutAValue() {
            // given metadata permits null values, which Map.copyOf rejects
            Map<String, String> metadata = new HashMap<>();
            metadata.put("tenant", null);

            // when / then
            assertThat(WireCodec.copyOf(metadata)).containsEntry("tenant", null);
        }

        @Test
        void treatsAbsentMetadataAsEmpty() {
            assertThat(WireCodec.copyOf(null)).isEmpty();
        }

        @Test
        void doesNotShareTheCopyWithTheOriginal() {
            // given
            Map<String, String> metadata = new HashMap<>();
            metadata.put("tenant", "acme");
            Map<String, String> copy = WireCodec.copyOf(metadata);

            // when
            metadata.put("tenant", "other");

            // then
            assertThat(copy).containsEntry("tenant", "acme");
        }
    }

    @Nested
    class DescribingAFailure {

        @Test
        void namesTheTypeOfAFailureCarryingNoMessage() {
            assertThat(WireCodec.messageOf(new IllegalStateException()))
                    .isEqualTo(IllegalStateException.class.getName());
        }

        @Test
        void describesTheCausesBehindAFailureOutermostFirst() {
            // given
            Throwable cause = new IllegalStateException("Outer.", new IllegalArgumentException("Inner."));

            // when / then
            assertThat(WireCodec.descriptionsOf(cause)).containsExactly("Outer.", "Inner.");
        }

        @Test
        void stopsDescribingAChainThatWouldNeverEnd() {
            // given a chain longer than any failure worth reporting in full
            Throwable deepest = new IllegalStateException("cause-0");
            Throwable cause = deepest;
            for (int i = 1; i <= 50; i++) {
                cause = new IllegalStateException("cause-" + i, cause);
            }

            // when / then a failure must not turn into an unbounded reply
            assertThat(WireCodec.descriptionsOf(cause)).hasSize(10);
        }

        @Test
        void stopsDescribingAFailureThatCausedItself() {
            // given a cycle, which a chain walked naively would follow forever
            SelfCausingException cause = new SelfCausingException();

            // when / then
            assertThat(WireCodec.descriptionsOf(cause)).hasSize(1);
        }
    }

    @Nested
    class CarryingFailureDetails {

        @Test
        void carriesDetailsAlreadyInBytesAsTheyAre() {
            // given
            byte[] details = "{\"reason\":\"closed\"}".getBytes(StandardCharsets.UTF_8);
            Throwable cause = new QueryExecutionException("Rejected.", null, details);

            // when / then
            assertThat(WireCodec.serializedDetailsOf(cause, null)).isEqualTo(details);
        }

        @Test
        void carriesNothingForAFailureWithoutDetails() {
            assertThat(WireCodec.serializedDetailsOf(new IllegalStateException("Plain."), null)).isNull();
        }

        @Test
        void omitsDetailsThatCannotBeConverted() {
            // given a failure whose details the converter cannot write
            Throwable cause = new QueryExecutionException("Rejected.", null, new Object());

            // when / then reporting the failure matters more than reporting its details in full
            assertThat(WireCodec.serializedDetailsOf(cause, new FailingConverter())).isNull();
        }

        @Test
        void omitsDetailsWhenNoConverterIsAvailable() {
            // given
            Throwable cause = new QueryExecutionException("Rejected.", null, new Object());

            // when / then
            assertThat(WireCodec.serializedDetailsOf(cause, null)).isNull();
        }
    }

    /**
     * A failure that reports itself as its own cause, as a chain walked naively would follow forever.
     */
    private static class SelfCausingException extends RuntimeException {

        private SelfCausingException() {
            super("cycle");
        }

        @Override
        public synchronized Throwable getCause() {
            return this;
        }
    }

    /**
     * A converter that cannot write anything, standing in for details of a type the application cannot serialize.
     */
    private static class FailingConverter implements Converter {

        @Override
        public <T> T convert(Object input, Type targetType) {
            throw new ConversionException("Cannot convert " + input.getClass().getName() + ".");
        }

        @Override
        public void describeTo(ComponentDescriptor descriptor) {
            descriptor.describeProperty("converts", "nothing");
        }
    }
}
