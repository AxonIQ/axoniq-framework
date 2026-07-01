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

package io.axoniq.framework.axonserver.connector.util;

import org.axonframework.common.util.PriorityTask;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Responsible for creating {@link Scheduler} implementations relevant to the Axon Server connector.
 *
 * @author Mitchell Herrijgers
 * @author Milan Savic
 * @since 4.6.0
 */
public interface PriorityTaskSchedulers {

    /**
     * Creates a {@link Scheduler} that is compatible with an {@link ExecutorService} that needs tasks that are
     * submitted to be a {@link PriorityTask} so that they can be prioritized.
     *
     * @param delegate     The delegate {@link ExecutorService} to use when submitting tasks.
     * @param priority     The priority that any tasks submitted to the delegate will have.
     * @param taskSequence The task sequence used for ordering items with the same priority.
     * @return The {@link Scheduler} object that is compatible with
     * {@link PriorityTask}.
     * @see PriorityExecutorService
     */
    static Scheduler forPriority(ExecutorService delegate, long priority, AtomicLong taskSequence) {
        return Schedulers.fromExecutor(new PriorityExecutorService(delegate, priority, taskSequence));
    }
}
