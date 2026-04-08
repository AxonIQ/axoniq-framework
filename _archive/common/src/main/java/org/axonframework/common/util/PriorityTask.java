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

/**
 * Represents a task such as {@link Runnable} or {@link Comparable} that adheres to a priority by implementing
 * {@link Comparable}. Uses a combination of {@code priority} and {@code index} to compare between {@code this} and
 * other {@link PriorityTask} instances. A calculator  (e.g.
 * {@link org.axonframework.commandhandling.CommandPriorityCalculator})  defines the priority of the task. This task
 * uses the {@code index} to differentiate between tasks with the same priority, ensuring the insert order is leading in
 * those scenarios.
 *
 * @author Stefan Dragisic
 * @author Milan Savic
 * @author Allard Buijze
 * @author Steven van Beelen
 * @author Mitchell Herrijgers
 * @since 4.6.0
 */
public interface PriorityTask extends Comparable<PriorityTask> {

    /**
     * Returns the priority of this task.
     *
     * @return The priority of this task.
     */
    long priority();

    /**
     * Returns the sequence of this task.
     *
     * @return The sequence of this task.
     */
    long sequence();

    @Override
    default int compareTo(PriorityTask that) {
        int c = Long.compare(this.priority(), that.priority());
        if (c != 0) {
            return -c;
        }
        return Long.compare(this.sequence(), that.sequence());
    }
}
