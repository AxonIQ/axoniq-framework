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

package org.axonframework.messaging.core;

import java.util.Optional;
import java.util.function.Consumer;

/**
 * Implementation of the {@link MessageStream} that ignores all {@link Entry entries} of the {@code delegate} stream and
 * returns an empty stream.
 * <p>
 * This allows users to define a {@code MessageStream} of any type and force it to a
 * {@link MessageStream.Empty} stream instance, effectively ignoring the results while
 * maintaining the processing of the stream.
 *
 * @param <M> The type of {@link Message} from the delegate stream that will be ignored.
 * @author Mateusz Nowak
 * @since 5.0.0
 */
class IgnoredEntriesMessageStream<M extends Message>
        extends DelegatingMessageStream<M, Message>
        implements MessageStream.Empty<Message> {

    private final Empty<Message> empty;

    /**
     * Constructs the IgnoreMessageStream with given {@code delegate} to receive and ignore entries from.
     *
     * @param delegate The instance to delegate calls to.
     */
    IgnoredEntriesMessageStream(MessageStream<M> delegate) {
        super(delegate);
        this.empty = MessageStream.empty();
    }

    @Override
    public Optional<Entry<Message>> next() {
        return delegate().next().flatMap(r -> Optional.empty());
    }

    @Override
    public Optional<Entry<Message>> peek() {
        return Optional.empty();
    }

    @Override
    public Empty<Message> onNext(Consumer<Entry<Message>> onNext) {
        return empty.onNext(onNext);
    }

}
