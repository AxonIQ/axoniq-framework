/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
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
