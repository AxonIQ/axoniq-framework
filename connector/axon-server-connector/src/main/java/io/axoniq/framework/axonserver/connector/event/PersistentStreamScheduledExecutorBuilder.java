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

package io.axoniq.framework.axonserver.connector.event;

import org.axonframework.common.AxonThreadFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiFunction;

/**
 * Functional interface towards constructing a {@link ScheduledExecutorService} for a {@link PersistentStreamMessageSource}.
 *
 * @author Steven van Beelen
 * @since 4.10.1
 */
@FunctionalInterface
public interface PersistentStreamScheduledExecutorBuilder
        extends BiFunction<Integer, String, ScheduledExecutorService> {

    /**
     * Builds a {@link ScheduledExecutorService} using the given {@code threadCount} and {@code streamName}.
     *
     * @param threadCount The requested thread count for the persistent stream. Can for example be used to define the
     *                    pool size of the {@link ScheduledExecutorService} under construction.
     * @param streamName The name of the persistent stream. Can, for example, be used to define the name of the
     * {@link java.util.concurrent.ThreadFactory} given to a {@link ScheduledExecutorService}
     * @return A {@link ScheduledExecutorService} based on the given {@code threadCount} and {@code streamName}.
     */
    default ScheduledExecutorService build(Integer threadCount, String streamName) {
        return apply(threadCount, streamName);
    }

    /**
     * Default {@link PersistentStreamScheduledExecutorBuilder}. Constructs a {@link ScheduledExecutorService} by using
     * the given {@code threadCount} as the pool size for the executor. Uses the given {@code streamName} to build an
     * {@link AxonThreadFactory} with the group name {@code "PersistentStream[{streamName}]"}.
     *
     * @return The default {@link PersistentStreamScheduledExecutorBuilder}.
     */
    static PersistentStreamScheduledExecutorBuilder defaultFactory() {
        return (threadCount, streamName) -> Executors.newScheduledThreadPool(
                threadCount, new AxonThreadFactory("PersistentStream[" + streamName + "]")
        );
    }
}
