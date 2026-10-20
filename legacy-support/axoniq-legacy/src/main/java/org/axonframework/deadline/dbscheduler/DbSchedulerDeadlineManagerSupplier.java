/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package org.axonframework.deadline.dbscheduler;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.jspecify.annotations.Nullable;

/**
 * Default supplier for an {@link DbSchedulerDeadlineManager}. This makes it easier to use in context without more
 * advanced ways of dependency injection. It can be passed to the tasks in the {@link DbSchedulerDeadlineManager} to
 * create the {@link com.github.kagkarlsson.scheduler.Scheduler} before the {@link DbSchedulerDeadlineManager} is
 * created. After creating it should be set on this {@link DbSchedulerDeadlineManagerSupplier}.
 *
 * @author Gerard Klijs
 * @since 4.9.0
 */
public class DbSchedulerDeadlineManagerSupplier implements Supplier<DbSchedulerDeadlineManager> {

    private final AtomicReference<@Nullable DbSchedulerDeadlineManager> deadlineManager = new AtomicReference<>();

    /**
     * Returns the set {@link DbSchedulerDeadlineManager}, or {@code null} if it hasn't been set yet.
     *
     * @return the set {@link DbSchedulerDeadlineManager}, or {@code null} if it hasn't been set yet
     */
    @Override
    public @Nullable DbSchedulerDeadlineManager get() {
        return deadlineManager.get();
    }

    /**
     * Sets the {@link DbSchedulerDeadlineManager} so the tasks created in advanced can access it.
     *
     * @param deadlineManager the {@link DbSchedulerDeadlineManager}
     */
    public void set(
            DbSchedulerDeadlineManager deadlineManager
    ) {
        this.deadlineManager.set(deadlineManager);
    }
}
