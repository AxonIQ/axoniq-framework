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

package org.axonframework.common.util;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.function.BiFunction;

/**
 * A Functional Interface towards a {@link BiFunction} which ingests both a Configuration and a {@link BlockingQueue} of
 * {@link Runnable}, and outputs an {@link ExecutorService}. Provides a means to allow configuration of the used
 * {@code ExecutorService} in, for example, the {@link org.axonframework.commandhandling.distributed.DistributedCommandBus}, but maintaining the option for the
 * framework to provide a {@code BlockingQueue} which is tailored towards message prioritization when building the
 * executor.
 * <p>
 * Before 5.0.0 this class was specific for the Axon Server configuration, but it has been generalized to allow other
 * configurations to provide their own {@code ExecutorService} implementations as well.
 *
 * @param <C> The type of configuration to use for constructing a {@link ExecutorService}.
 * @author Steven van Beelen
 * @since 5.0.0
 */
@FunctionalInterface
public interface ExecutorServiceFactory<C> {

    /**
     * Creates an {@link ExecutorService} based on the given {@code configuration} and {@code queue}.
     *
     * @param configuration The Configuration to use for the ExecutorService.
     * @param queue         The {@link BlockingQueue} to use for the ExecutorService.
     * @return An {@link ExecutorService}based on the given {@code configuration} and {@code queue}.
     */
    ExecutorService createExecutorService(C configuration, BlockingQueue<Runnable> queue);
}