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
package io.axoniq.framework.workflow.simulation.harness;


import java.time.Duration;
import java.util.function.BooleanSupplier;

/**
 * Minimal condition-polling used by the harness to wait for the (real, asynchronous) Axon event processor to finish
 * routing a delivered event before the next simulation step.
 * <p>
 * The harness lives in main scope and therefore cannot use the test-scoped Awaitility; this is the small in-house
 * equivalent. It does not introduce nondeterminism into the run: it only bounds how long the harness waits for the
 * processor thread to reach a stable point — the committed log and state it then inspects are a deterministic
 * function of the events delivered so far.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class Polling {

    private static final Duration POLL_INTERVAL = Duration.ofMillis(2);

    private Polling() {
    }

    /**
     * Waits until {@code condition} holds or the timeout elapses.
     *
     * @param timeout   maximum time to wait.
     * @param condition the condition to await.
     * @return {@code true} if the condition held within the timeout, {@code false} otherwise.
     */
    public static boolean await(Duration timeout, BooleanSupplier condition) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return true;
            }
            try {
                Thread.sleep(POLL_INTERVAL.toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return condition.getAsBoolean();
            }
        }
        return condition.getAsBoolean();
    }

    /**
     * Waits until {@code condition} holds, throwing {@link IllegalStateException} (with {@code description}) if the
     * timeout elapses first.
     *
     * @param timeout     maximum time to wait.
     * @param description what was being awaited, for the error message.
     * @param condition   the condition to await.
     */
    public static void awaitOrFail(Duration timeout, String description,
                                   BooleanSupplier condition) {
        if (!await(timeout, condition)) {
            throw new IllegalStateException("Timed out after " + timeout + " waiting for: " + description);
        }
    }
}
