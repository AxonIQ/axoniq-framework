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

package org.axonframework.deadline;

import org.jspecify.annotations.Nullable;

import java.util.Objects;

/**
 * The payload of a fired deadline whose stored payload type name could not be resolved to a class.
 * <p>
 * The persistent deadline managers ({@link org.axonframework.deadline.quartz.QuartzDeadlineManager},
 * {@link org.axonframework.deadline.jobrunr.JobRunrDeadlineManager} and
 * {@link org.axonframework.deadline.dbscheduler.DbSchedulerDeadlineManager}) store a deadline's payload together with
 * the name of its type. When a deadline fires on a node that lacks that type, for example because the payload class was
 * renamed or moved while the deadline was pending, the deadline fires with this payload instead of failing, and the
 * job completes. Axon Framework 4 did the same through its {@code UnknownSerializedType}.
 * <p>
 * A {@link org.axonframework.deadline.annotation.DeadlineHandler} without a payload parameter receives it. A handler
 * that declares another payload type does not. To read the content, convert the {@link #data()} with the
 * {@link org.axonframework.conversion.Converter} the deadline manager was configured with:
 * <pre>{@code
 * @DeadlineHandler(deadlineName = "paymentDue")
 * public void on(DeadlineMessage deadline) {
 *     if (deadline.payload() instanceof UnknownDeadlinePayload unknown) {
 *         logger.warn("Deadline with payload type [{}] fired, but that type is unknown", unknown.typeName());
 *     }
 * }
 * }</pre>
 *
 * @param typeName the stored name of the payload type, which could not be resolved
 * @param revision the stored revision of the payload type, if any
 * @param data     the stored payload, as a {@code byte[]} or a {@code String}, depending on the deadline manager
 * @author Jakob Hatzl
 * @since 5.4.0
 */
public record UnknownDeadlinePayload(String typeName, @Nullable String revision, @Nullable Object data) {

    /**
     * Creates an {@code UnknownDeadlinePayload} for the given stored {@code typeName}, {@code revision} and
     * {@code data}.
     *
     * @param typeName the stored name of the payload type, which could not be resolved
     * @param revision the stored revision of the payload type, if any
     * @param data     the stored payload, as a {@code byte[]} or a {@code String}, depending on the deadline manager
     */
    public UnknownDeadlinePayload {
        Objects.requireNonNull(typeName, "The type name may not be null.");
    }
}
