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

package io.axoniq.framework.axonserver.connector.api;

import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * A no-op implementation of the {@link StreamObserver}, to be used in test scenarios where we do not care about the
 * exact implementation, but want to perform simple assertions instead.
 *
 * @param <T> the type this {@link StreamObserver} implementation handles {@link #onNext(Object)}
 * @author Steven van Beelen
 */
public class TestStreamObserver<T> implements StreamObserver<T> {

    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final boolean TOGGLED_LOGGING_OFF = false;

    private final boolean logging;

    private final Collection<T> sentMessages = new ConcurrentLinkedQueue<>();

    /**
     * Build a default NoOpStreamObserver that logs when it reaches the {@link #onNext(Object)}, {@link
     * #onError(Throwable)} and {@link #onCompleted()} methods.
     */
    public TestStreamObserver() {
        this(TOGGLED_LOGGING_OFF);
    }

    /**
     * Gets messages that are sent by this stream.
     *
     * @return messages that are sent by this stream
     */
    public Collection<T> sentMessages() {
        return sentMessages;
    }

    /**
     * Build a NoOpStreamObserver where the given {@code toggledLogging} defines whether a message is logged when
     * {@link #onNext(Object)}, {@link #onError(Throwable)} or {@link #onCompleted()} is called.
     *
     * @param toggledLogging a boolean specifying whether logging is toggled off or on
     */
    public TestStreamObserver(boolean toggledLogging) {
        this.logging = toggledLogging;
    }

    @Override
    public void onNext(T t) {
        if (logging) {
            logger.info("Handling onNext operation with the following input: \n{}", t);
        }
        sentMessages.add(t);
    }

    @Override
    public void onError(Throwable throwable) {
        if (logging) {
            logger.info("Handling onError operation", throwable);
        }
    }

    @Override
    public void onCompleted() {
        if (logging) {
            logger.info("Handling onCompleted operation");
        }
    }
}
