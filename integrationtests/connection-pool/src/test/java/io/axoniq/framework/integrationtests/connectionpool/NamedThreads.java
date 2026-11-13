/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.integrationtests.connectionpool;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A {@link ThreadFactory} creating daemon threads with a recognizable name, so tests can tell which thread did what.
 *
 * @author John Hendrikx
 */
final class NamedThreads implements ThreadFactory {

    private final String prefix;
    private final AtomicInteger counter = new AtomicInteger();

    /**
     * Constructs a new instance.
     *
     * @param prefix the prefix of the thread names
     */
    NamedThreads(String prefix) {
        this.prefix = prefix;
    }

    @Override
    public Thread newThread(Runnable runnable) {
        Thread thread = new Thread(runnable, prefix + counter.incrementAndGet());

        thread.setDaemon(true);

        return thread;
    }
}
