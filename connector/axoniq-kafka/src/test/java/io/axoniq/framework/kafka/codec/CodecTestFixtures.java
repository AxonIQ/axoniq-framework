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

package io.axoniq.framework.kafka.codec;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serde;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.conversion.DelegatingEventConverter;
import org.jspecify.annotations.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.function.Function;

/**
 * Codecs and record headers that several Kafka tests share.
 *
 * @author Allard Buijze
 */
public final class CodecTestFixtures {

    private CodecTestFixtures() {
    }

    /**
     * Returns the default codec, converting payloads with Jackson.
     */
    public static DefaultKafkaEventCodec defaultCodec() {
        return new DefaultKafkaEventCodec(new DelegatingEventConverter(new JacksonConverter()));
    }

    /**
     * Returns a codec that encodes as the given {@code delegate} does, and decodes with the given {@code decoder}.
     */
    public static KafkaEventCodec<byte[]> decodingWith(
            KafkaEventCodec<byte[]> delegate,
            Function<ConsumerRecord<String, byte[]>, Optional<DecodedRecord>> decoder
    ) {
        return new KafkaEventCodec<>() {
            @Override
            public ProducerRecord<String, byte[]> encode(EventMessage event, String topic, @Nullable String key,
                                                         @Nullable ProcessingContext context) {
                return delegate.encode(event, topic, key, context);
            }

            @Override
            public Optional<DecodedRecord> decode(ConsumerRecord<String, byte[]> record) {
                return decoder.apply(record);
            }

            @Override
            public Serde<byte[]> valueSerde() {
                return delegate.valueSerde();
            }
        };
    }

    /**
     * Returns a codec that throws the given {@code failure} when decoding the record at the given {@code offset}, and
     * otherwise works as the given {@code delegate} does.
     */
    public static KafkaEventCodec<byte[]> failingAt(KafkaEventCodec<byte[]> delegate,
                                                    long offset,
                                                    RuntimeException failure) {
        return decodingWith(delegate, record -> {
            if (record.offset() == offset) {
                throw failure;
            }
            return delegate.decode(record);
        });
    }

    /**
     * Replaces the event timestamp header that the default codec writes with text that is no timestamp, making the
     * record malformed.
     */
    public static void makeTimestampUnreadable(Headers headers) {
        headers.remove(DefaultKafkaEventCodec.TIMESTAMP_HEADER);
        headers.add(DefaultKafkaEventCodec.TIMESTAMP_HEADER, "yesterday".getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Returns the value of the last header with the given {@code key}, as UTF-8 text, or {@code null} when there is
     * none.
     */
    public static @Nullable String header(Headers headers, String key) {
        Header header = headers.lastHeader(key);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }
}
