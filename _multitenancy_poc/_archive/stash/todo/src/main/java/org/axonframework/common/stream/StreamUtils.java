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

package org.axonframework.common.stream;

import org.axonframework.messaging.eventhandling.TrackingEventStream;

import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static java.util.Spliterator.*;
import static java.util.stream.StreamSupport.stream;

/**
 * Utility class for working with Streams.
 */
public abstract class StreamUtils {

    private StreamUtils() {
    }

    /**
     * Convert the given {@code messageStream} to a regular java {@link Stream}. Note that the returned
     * stream will block during iteration if the end of the stream is reached so take heed of this in production code.
     *
     * Closing this {@code Stream} will close the underling {@code messageStream} as well.
     *
     * @param messageStream the input {@link TrackingEventStream}
     * @return the output {@link Stream} after conversion
     * @param <M> The type of entry contained in the stream
     */
    public static <M> Stream<M> asStream(BlockingStream<M> messageStream) {
        Spliterator<M> spliterator =
                new Spliterators.AbstractSpliterator<>(Long.MAX_VALUE, DISTINCT | NONNULL | ORDERED) {
                    @Override
                    public boolean tryAdvance(Consumer<? super M> action) {
                        try {
                            action.accept(messageStream.nextAvailable());
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return false;
                        }
                        return true;
                    }
                };
        return stream(spliterator, false).onClose(messageStream::close);
    }
}
