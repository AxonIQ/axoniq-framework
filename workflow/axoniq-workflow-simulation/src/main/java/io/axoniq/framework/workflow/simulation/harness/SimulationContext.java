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


import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Everything a {@link io.axoniq.framework.workflow.simulation.faults.Fault} needs: the {@link SimulationWorld}, the single seeded
 * RNG that determines the whole run, the queue of external events still to be delivered, and a trace recorder.
 * <p>
 * All randomness in a run flows through {@link #rng()} so a seed fully determines the sequence of fault choices and
 * any randomized fault parameters (which event to duplicate, how big a clock jump is).
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class SimulationContext {

    private final SimulationWorld world;
    private final Random rng;
    private final List<PendingEvent> pendingEvents;
    private final List<String> trace = new ArrayList<>();

    /**
     * Creates a context.
     *
     * @param world         the simulated world.
     * @param rng           the single seeded RNG driving the run.
     * @param pendingEvents mutable list of external events still to deliver (faults may reorder/duplicate it).
     */
    public SimulationContext(SimulationWorld world, Random rng,
                             List<PendingEvent> pendingEvents) {
        this.world = world;
        this.rng = rng;
        this.pendingEvents = pendingEvents;
    }

    /**
     * Returns the simulated world.
     *
     * @return the world.
     */
        public SimulationWorld world() {
        return world;
    }

    /**
     * Returns the single seeded RNG. All fault choices and parameters must be drawn from here.
     *
     * @return the RNG.
     */
        public Random rng() {
        return rng;
    }

    /**
     * Returns the mutable list of external events still pending delivery.
     *
     * @return pending events.
     */
        public List<PendingEvent> pendingEvents() {
        return pendingEvents;
    }

    /**
     * Records a human-readable trace line (used to print the seed→fault-sequence on failure and for Phase-5 hand-off).
     *
     * @param line the trace line.
     */
    public void record(String line) {
        trace.add(line);
    }

    /**
     * Returns an immutable copy of the trace recorded so far.
     *
     * @return the trace lines in order.
     */
        public List<String> trace() {
        return List.copyOf(trace);
    }

    /**
     * An external event scheduled for delivery, optionally delayed by a number of simulation steps.
     *
     * @param event       the event payload object to publish.
     * @param delaySteps  how many further steps to wait before delivering (0 = deliver at the next delivery point).
     * @param description short label for tracing.
     */
    public record PendingEvent(Object event, int delaySteps, String description) {

        /**
         * Returns a copy of this pending event with its delay decremented by one (not below zero).
         *
         * @return the aged pending event.
         */
                public PendingEvent aged() {
            return new PendingEvent(event, Math.max(0, delaySteps - 1), description);
        }

        /**
         * Returns a copy with an added delay.
         *
         * @param extraSteps additional steps to delay.
         * @return the delayed pending event.
         */
                public PendingEvent delayedBy(int extraSteps) {
            return new PendingEvent(event, delaySteps + extraSteps, description);
        }
    }
}
