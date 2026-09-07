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

import org.jspecify.annotations.Nullable;

import java.lang.reflect.Constructor;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Phase 2 spike — a deterministic virtual-thread executor: every task submitted to it runs on a virtual thread
 * whose continuations are executed by a SINGLE carrier thread, in an order this class controls.
 * <p>
 * This is the whole-system-determinism building block: with one carrier there is no thread-level parallelism among
 * engine bodies, so the interleaving of body steps becomes a pure function of the run-queue pick order. Two modes:
 * <ul>
 *   <li><b>FIFO (default)</b> — continuations run in ready order; the closest single-threaded approximation of the
 *       production virtual-thread-per-task executor.</li>
 *   <li><b>Seeded ({@code new DeterministicVirtualThreadExecutor(seed)})</b> — the next continuation is picked
 *       seeded-randomly among the ready set: a reproducible <i>interleaving fuzz</i> dimension. The same seed picks
 *       the same schedule whenever the ready sets evolve identically.</li>
 * </ul>
 * <b>Scope and honesty:</b> this controls ONLY tasks routed through this executor (workflow bodies + engine event
 * publication offload — everything the engine sends to {@code WORKFLOW_ENGINE_EXECUTOR}). Axon's pooled-streaming
 * processor threads, the settle machinery and the JUnit thread remain platform threads outside this scheduler, so
 * enqueue timing from those sources is still environmental. What single-carrier execution removes is body-vs-body
 * and body-vs-publish parallelism — the dimension the F-20 class of races lives in.
 * <p>
 * <b>Mechanism:</b> reflects the JDK's {@code ThreadBuilders$VirtualThreadBuilder(Executor)} test constructor to
 * mount virtual threads on the carrier (requires {@code --add-opens java.base/java.lang=ALL-UNNAMED}, wired in the
 * simulation module's surefire config). A virtual thread that blocks (queue take, park) unmounts and frees the
 * carrier; JDK-21 monitor pinning ({@code synchronized} + block) would stall the carrier — the harness deadline
 * turns that into a visible abort, not a hang.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class DeterministicVirtualThreadExecutor extends AbstractExecutorService implements AutoCloseable {

    private final Object lock = new Object();
    private final List<Runnable> ready = new ArrayList<>();
    private final Random pickOrder; // null => FIFO
    private final Thread carrier;
    private final AtomicInteger liveTasks = new AtomicInteger();
    private final AtomicLong scheduled = new AtomicLong();
    private final Thread.Builder vtBuilder;
    private volatile boolean shutdown;

    /** FIFO pick order — deterministic single-carrier serialization of the production executor. */
    public DeterministicVirtualThreadExecutor() {
        this(null);
    }

    /** Seeded pick order — reproducible interleaving fuzz over the ready set. */
    public DeterministicVirtualThreadExecutor(long interleavingSeed) {
        this(new Random(interleavingSeed));
    }

    private DeterministicVirtualThreadExecutor(@Nullable Random pickOrder) {
        this.pickOrder = pickOrder;
        this.vtBuilder = reflectVirtualBuilder(this::enqueueContinuation);
        this.carrier = new Thread(this::carrierLoop, "dst-carrier");
        this.carrier.setDaemon(true);
        this.carrier.start();
    }

    private static Thread.Builder reflectVirtualBuilder(Executor continuationScheduler) {
        try {
            Class<?> clazz = Class.forName("java.lang.ThreadBuilders$VirtualThreadBuilder");
            Constructor<?> ctor = clazz.getDeclaredConstructor(Executor.class);
            ctor.setAccessible(true);
            return (Thread.Builder) ctor.newInstance(continuationScheduler);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(
                    "DeterministicVirtualThreadExecutor needs the JDK VirtualThreadBuilder(Executor) constructor "
                            + "and --add-opens java.base/java.lang=ALL-UNNAMED (see workflow/axoniq-workflow-simulation/pom.xml surefire "
                            + "argLine). Underlying error: " + e, e);
        }
    }

    private void enqueueContinuation(Runnable continuation) {
        synchronized (lock) {
            ready.add(continuation);
            scheduled.incrementAndGet();
            lock.notifyAll();
        }
    }

    private void carrierLoop() {
        while (true) {
            Runnable next;
            synchronized (lock) {
                while (ready.isEmpty()) {
                    if (shutdown && liveTasks.get() == 0) {
                        return;
                    }
                    try {
                        lock.wait(50L);
                    } catch (InterruptedException e) {
                        return;
                    }
                }
                next = (pickOrder == null) ? ready.remove(0) : ready.remove(pickOrder.nextInt(ready.size()));
            }
            next.run(); // runs the virtual thread until it blocks, yields or terminates
        }
    }

    @Override
    public void execute(Runnable command) {
        if (shutdown) {
            throw new java.util.concurrent.RejectedExecutionException("executor is shut down");
        }
        liveTasks.incrementAndGet();
        vtBuilder.start(() -> {
            try {
                command.run();
            } finally {
                liveTasks.decrementAndGet();
            }
        });
    }

    /**
     * Waits until no continuation is ready or mounted (the scheduler's notion of quiescence), or the budget elapses.
     *
     * @param budget maximum wait.
     * @return {@code true} when quiescent within the budget.
     */
    public boolean awaitQuiescence(Duration budget) {
        long deadline = System.nanoTime() + budget.toNanos();
        long lastScheduled = -1L;
        while (System.nanoTime() < deadline) {
            synchronized (lock) {
                long now = scheduled.get();
                if (ready.isEmpty() && now == lastScheduled) {
                    return true;
                }
                lastScheduled = now;
            }
            try {
                Thread.sleep(2L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return false;
    }

    /** Continuation dispatch count — a cheap schedule fingerprint component. */
    public long scheduledCount() {
        return scheduled.get();
    }

    @Override
    public void shutdown() {
        shutdown = true;
        synchronized (lock) {
            lock.notifyAll();
        }
    }

        @Override
    public List<Runnable> shutdownNow() {
        shutdown();
        return List.of();
    }

    @Override
    public boolean isShutdown() {
        return shutdown;
    }

    @Override
    public boolean isTerminated() {
        return shutdown && liveTasks.get() == 0;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        while (!isTerminated() && System.nanoTime() < deadline) {
            Thread.sleep(2L);
        }
        return isTerminated();
    }

    @Override
    public void close() {
        shutdown();
    }
}
