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
