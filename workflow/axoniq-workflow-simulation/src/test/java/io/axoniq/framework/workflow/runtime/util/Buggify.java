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
package io.axoniq.framework.workflow.runtime.util;


import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

/**
 * BUGGIFY seam (Phase 4) — FoundationDB-style in-engine fault points that bias execution toward rare interleavings,
 * strictly test-activated.
 * <p>
 * Production: {@link #fire(String)} is a single {@code null} check on a {@code volatile} field — no behaviour, no
 * allocation, nothing to configure. Activation happens ONLY from test/simulation code via
 * {@link #activate(long, double)}; each named point then perturbs scheduling (a yield, occasionally a sub-millisecond
 * park) with the given seeded probability, and counts how often it fired. This makes boundary windows (live-switch,
 * task-append) get visited far more often per run than the OS scheduler would ever produce naturally.
 * <p>
 * The state is JVM-global (like the aligned-clock seam): activate/deactivate around one world at a time; the DST
 * suites are single-threaded per JVM. Always deactivate in a {@code finally}.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class Buggify {

    private static volatile State state;

    private Buggify() {
    }

    /**
     * A BUGGIFY point: no-op unless {@link #activate(long, double)} was called; when active, perturbs scheduling with
     * the activation probability and counts the firing under {@code point}.
     *
     * @param point stable name of the fault point (e.g. {@code "engine.live-switch"}).
     */
    public static void fire(String point) {
        State s = state;
        if (s == null) {
            return;
        }
        s.fire(point);
    }

    /**
     * Activates every BUGGIFY point with the given seeded probability. Test/simulation use only.
     *
     * @param seed        seed for the per-firing decisions (reproducible given identical firing sequences).
     * @param probability probability in [0,1] that a reached point perturbs scheduling.
     */
    public static void activate(long seed, double probability) {
        state = new State(new Random(seed), probability);
    }

    /**
     * Deactivates all points (returns {@link #fire(String)} to the production no-op) and returns the firing counts.
     *
     * @return how often each point actually perturbed scheduling while active.
     */
        public static Map<String, Integer> deactivate() {
        State s = state;
        state = null;
        return s == null ? Map.of() : s.snapshot();
    }

    private static final class State {

        private final Random rng;
        private final double probability;
        private final ConcurrentHashMap<String, AtomicInteger> fired = new ConcurrentHashMap<>();

        private State(Random rng, double probability) {
            this.rng = rng;
            this.probability = probability;
        }

        private void fire(String point) {
            boolean perturb;
            synchronized (rng) {
                perturb = rng.nextDouble() < probability;
            }
            if (!perturb) {
                return;
            }
            fired.computeIfAbsent(point, k -> new AtomicInteger()).incrementAndGet();
            // Scheduling bias: yield; every so often a sub-millisecond park to widen the window further.
            Thread.yield();
            if (fired.get(point).get() % 4 == 0) {
                LockSupport.parkNanos(200_000L);
            }
        }

        private Map<String, Integer> snapshot() {
            var out = new java.util.HashMap<String, Integer>();
            fired.forEach((k, v) -> out.put(k, v.get()));
            return Map.copyOf(out);
        }
    }
}
