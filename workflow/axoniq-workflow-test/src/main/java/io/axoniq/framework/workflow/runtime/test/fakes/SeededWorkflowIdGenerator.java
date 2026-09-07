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
package io.axoniq.framework.workflow.runtime.test.fakes;


import java.util.concurrent.atomic.AtomicLong;

/**
 * Deterministic workflow id generator fake producing a reproducible sequence of identifiers from a seed.
 * <p>
 * Each call to {@link #newId()} returns {@code <prefix>-<n>} where {@code n} starts at the configured seed and
 * increments by one. Given the same seed and the same number of calls in the same order, the generated identifiers are
 * identical across runs, which lets a deterministic simulator reproduce a run bit-for-bit.
 * <p>
 * Register this in place of the default {@code RandomWorkflowIdGenerator} via the configurer:
 * {@code componentRegistry(cr -> cr.registerComponent(WorkflowIdGenerator.class, cfg -> generator))}.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public class SeededWorkflowIdGenerator {

    private final String prefix;
    private final AtomicLong counter;

    /**
     * Creates a generator with prefix {@code "id"} starting at seed {@code 0}.
     */
    public SeededWorkflowIdGenerator() {
        this("id", 0L);
    }

    /**
     * Creates a generator starting at the given seed with prefix {@code "id"}.
     *
     * @param seed first value of the counter.
     */
    public SeededWorkflowIdGenerator(long seed) {
        this("id", seed);
    }

    /**
     * Creates a generator with the given prefix and seed.
     *
     * @param prefix prefix applied to every generated identifier.
     * @param seed   first value of the counter.
     */
    public SeededWorkflowIdGenerator(String prefix, long seed) {
        this.prefix = prefix;
        this.counter = new AtomicLong(seed);
    }

        public String newId() {
        return prefix + "-" + counter.getAndIncrement();
    }
}
