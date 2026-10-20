/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package org.axonframework.deadline.dbscheduler;

import org.axonframework.common.AxonException;

/**
 * Exception indicating a problem in the Deadline manager mechanism, more precisely the
 * {@link DbSchedulerDeadlineManager} wasn't properly supplied.
 *
 * @author Gerard Klijs
 * @since 4.8.0
 */
public class DeadlineManagerNotSuppliedException extends AxonException {

    /**
     * Initialize a DeadlineManagerNotSuppliedException.
     */
    public DeadlineManagerNotSuppliedException() {
        super("The DbSchedulerDeadlineManager is not properly supplied to execute the task.");
    }
}
