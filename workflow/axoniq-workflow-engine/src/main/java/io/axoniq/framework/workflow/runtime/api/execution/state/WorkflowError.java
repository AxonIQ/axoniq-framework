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
package io.axoniq.framework.workflow.runtime.api.execution.state;

import org.jspecify.annotations.Nullable;
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
 * @since 5.4.0
 */
@Internal
public record WorkflowError(String type,
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

    private static WorkflowError fromInternal(Throwable throwable,
                                              IdentityHashMap<Throwable, Boolean> seen,
                                              int depth) {
        seen.put(throwable, Boolean.TRUE);
        Throwable rawCause = throwable.getCause();
        WorkflowError compactCause = null;
        if (rawCause != null && rawCause != throwable && !seen.containsKey(rawCause) && depth + 1 < MAX_CAUSE_DEPTH) {
            compactCause = fromInternal(rawCause, seen, depth + 1);
        }
        String typeName = throwable instanceof Cause c ? c.type() : throwable.getClass().getName();
        String message = throwable instanceof Cause c ? c.message() : throwable.getMessage();
        return new WorkflowError(typeName, truncate(message), compactCause);
    }

    @Nullable
    private static String truncate(@Nullable String message) {
        if (message == null || message.length() <= TRUNCATED_MESSAGE_SIZE) {
            return message;
        }
        return message.substring(0, TRUNCATED_MESSAGE_SIZE);
    }

    /**
     * Rebuilds an exception chain from this compact representation.
     * <p>
     * Exceptions thrown by a step body are rebuilt as a stackless {@link WorkflowExecutionException} carrying the
     * original exception class FQN and message. The engine-raised {@link StepIndeterminateException} is rebuilt as its
     * own type, so a workflow body can catch it after replay exactly as during the run that recorded it. The cause (if
     * any) is reconstructed the same way.
     *
     * @return reconstructed exception.
     */
    public RuntimeException toThrowable() {
        RuntimeException reconstructedCause = cause != null ? cause.toThrowable() : null;
        if (StepIndeterminateException.class.getName().equals(type)) {
            return new StepIndeterminateException(message, reconstructedCause);
        }
        return new WorkflowExecutionException(type, message, reconstructedCause);
    }
}
