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
package io.axoniq.workflow.runtime.api.execution.state;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.axonframework.common.annotation.Internal;

import java.util.IdentityHashMap;

/**
 * Compact, serialization-friendly representation of a {@link Throwable} stored in workflow events.
 * <p>
 * Captures only the exception class name and message — the stack trace is deliberately omitted to keep event-store
 * payloads small and avoid bytecode-incompatibility issues across JVM versions. The full stack trace is still
 * available through the executor logs when the failure occurs.
 * <p>
 *
 * @param type    fully-qualified name of the originating throwable's class.
 * @param message message of the originating throwable, truncated to {@link #TRUNCATED_MESSAGE_SIZE} characters, or
 *                {@code null}.
 * @param cause   compacted cause, or {@code null}.
 *
 * @author Stefan Dragisic
 * @since 1.0.0
 */
@Internal
public record WorkflowError(@Nonnull String type,
                            @Nullable String message,
                            @Nullable WorkflowError cause) implements Cause {

    /**
     * Maximum number of cause-chain levels captured. Protects against deep chains.
     */
    public static final int MAX_CAUSE_DEPTH = 10;

    /**
     * Maximum length of {@link #message()} characters.
     */
    public static final int TRUNCATED_MESSAGE_SIZE = 1023;

    /**
     * Compacts the given throwable and its cause chain (up to {@link #MAX_CAUSE_DEPTH} levels) into a
     * {@link WorkflowError}. Messages longer than {@link #TRUNCATED_MESSAGE_SIZE} are truncated. Cycles are broken via
     * identity tracking.
     *
     * @param throwable throwable to compact, may be {@code null}.
     * @return compacted representation, or {@code null} if {@code throwable} is {@code null}.
     */
    @Nullable
    public static WorkflowError from(@Nullable Throwable throwable) {
        if (throwable == null) {
            return null;
        }
        return fromInternal(throwable, new IdentityHashMap<>(), 0);
    }

    @Nonnull
    private static WorkflowError fromInternal(@Nonnull Throwable throwable,
                                              @Nonnull IdentityHashMap<Throwable, Boolean> seen,
                                              int depth) {
        seen.put(throwable, Boolean.TRUE);
        Throwable rawCause = throwable.getCause();
        WorkflowError compactCause = null;
        if (rawCause != null && rawCause != throwable && !seen.containsKey(rawCause) && depth + 1 < MAX_CAUSE_DEPTH) {
            compactCause = fromInternal(rawCause, seen, depth + 1);
        }
        return new WorkflowError(throwable.getClass().getName(), truncate(throwable.getMessage()), compactCause);
    }

    @Nullable
    private static String truncate(@Nullable String message) {
        if (message == null || message.length() <= TRUNCATED_MESSAGE_SIZE) {
            return message;
        }
        return message.substring(0, TRUNCATED_MESSAGE_SIZE);
    }

    /**
     * Rebuilds a stackless {@link Throwable} chain from this compact representation. The returned throwable is a
     * {@link WorkflowExecutionException} carrying the original exception class FQN and message; its cause (if any)
     * is similarly reconstructed.
     *
     * @return reconstructed throwable.
     */
    @Nonnull
    public Throwable toThrowable() {
        Throwable reconstructedCause = cause != null ? cause.toThrowable() : null;
        return new WorkflowExecutionException(type, message, reconstructedCause);
    }
}
