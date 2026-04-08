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

package org.axonframework.common;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Thread factory that created threads in a given group.
 *
 * @author Allard Buijze
 * @since 2.0
 */
public class AxonThreadFactory implements ThreadFactory {

    private final int priority;
    private final ThreadGroup threadGroup;
    private final AtomicInteger threadNumber = new AtomicInteger();

    /**
     * Initializes a ThreadFactory instance that creates each thread in a group with given {@code groupName} with
     * default priority.
     *
     * @param groupName The name of the group to create each thread in
     * @see Thread#setPriority(int)
     */
    public AxonThreadFactory(String groupName) {
        this(new ThreadGroup(groupName));
    }

    /**
     * Initializes a ThreadFactory instance that create each thread in the given {@code group} with default
     * priority.
     *
     * @param group The ThreadGroup to create each thread in
     * @see Thread#setPriority(int)
     */
    public AxonThreadFactory(ThreadGroup group) {
        this(Thread.NORM_PRIORITY, group);
    }

    /**
     * Initializes a ThreadFactory instance that create each thread in the given {@code group} with given
     * {@code priority}.
     *
     * @param priority The priority of the threads to create
     * @param group    The ThreadGroup to create each thread in
     * @see Thread#setPriority(int)
     */
    public AxonThreadFactory(int priority, ThreadGroup group) {
        Assert.isTrue(priority <= Thread.MAX_PRIORITY && priority >= Thread.MIN_PRIORITY,
                      () -> "Given priority is invalid");
        this.priority = priority;
        this.threadGroup = group;
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread thread = new Thread(threadGroup, r, threadGroup.getName() + "-" + nextThreadNumber());
        thread.setPriority(priority);
        return thread;
    }

    private int nextThreadNumber() {
        return threadNumber.getAndIncrement();
    }
}
