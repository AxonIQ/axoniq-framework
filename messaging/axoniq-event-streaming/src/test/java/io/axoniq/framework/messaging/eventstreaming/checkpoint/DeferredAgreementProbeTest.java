/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.messaging.eventstreaming.checkpoint;

import org.axonframework.messaging.eventhandling.processing.streaming.pooled.SegmentProgressStrategyTestSupport;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.token.GlobalSequenceTrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.Invocation;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.mockito.Mockito.mockingDetails;

/**
 * Review probes for PR 804. Same file runs against main and the PR strategy; prints PROBE lines with measurements.
 */
class DeferredAgreementProbeTest extends SegmentProgressStrategyTestSupport {

    private final ScheduledExecutorService side = Executors.newScheduledThreadPool(4);
    private final String side_ = System.getProperty("probe.side", "?");

    @AfterEach
    void stopSide() {
        side.shutdownNow();
    }

    private WorkPackageHarness deferred(Checkpointing participant) {
        return harness(context -> new CheckpointingProgressStrategy(context, List.of(participant), false));
    }

    private List<Long> stored() {
        return mockingDetails(tokenStore()).getInvocations().stream()
                                           .filter(i -> i.getMethod().getName().equals("storeToken"))
                                           .map(Invocation::getArguments)
                                           .map(a -> ((TrackingToken) a[0]).position().orElse(-1))
                                           .toList();
    }

    private static TrackingToken at(long position) {
        return new GlobalSequenceTrackingToken(position);
    }

    private static long ms(long startNanos) {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
    }

    private static long waitFor(Supplier<Boolean> condition, long capMs) throws InterruptedException {
        long start = System.nanoTime();
        while (!condition.get()) {
            if (ms(start) > capMs) {
                return -1;
            }
            Thread.sleep(2);
        }
        return ms(start);
    }

    private void print(String probe, String text) {
        System.out.println("PROBE " + probe + " side=" + side_ + " " + text);
    }

    /** The issue scenario: the agreement on 5 is only reached once event 6 has been handed to the handler. */
    @Test
    void issueScenarioAgreementNeedsTheNextEvent() throws Exception {
        ProbeParticipant participant = new ProbeParticipant();
        WorkPackageHarness subject = deferred(participant);
        subject.onSegmentClaimed();
        for (long i = 1; i <= 5; i++) {
            subject.scheduleEvent(eventAt(i));
        }
        waitFor(() -> subject.batchProcessor().processed().size() == 5, 5_000);
        CompletableFuture<TrackingToken> safe = new CompletableFuture<>();
        participant.advance = requested -> safe;
        side.submit(() -> {
            while (subject.batchProcessor().processed().size() < 6) {
                Thread.sleep(1);
            }
            return safe.complete(at(5));
        });

        long start = System.nanoTime();
        participant.trigger.requestCheckpoint(at(5));
        waitFor(() -> participant.advanced.size() == 1, 2_000);
        Thread.sleep(20);
        subject.scheduleEvent(eventAt(6));
        long event6 = waitFor(() -> subject.batchProcessor().processed().size() >= 6, 40_000);
        long store5 = waitFor(() -> stored().contains(5L), Math.max(0, 40_000 - ms(start)));
        subject.scheduleEvent(eventAt(7));
        long event7 = waitFor(() -> subject.batchProcessor().processed().size() >= 7, 2_000);
        print("P1-issue", "event6HandedMs=" + event6 + " store5Ms=" + store5 + " event7HandedMs=" + event7
                + " stored=" + stored() + " totalMs=" + ms(start));
    }

    /** Control: a participant that agrees at once (the common case). */
    @Test
    void controlAgreementReachedAtOnce() throws Exception {
        ProbeParticipant participant = new ProbeParticipant();
        WorkPackageHarness subject = deferred(participant);
        subject.onSegmentClaimed();
        subject.scheduleEvent(eventAt(1));
        waitFor(() -> subject.batchProcessor().processed().size() == 1, 5_000);
        long start = System.nanoTime();
        participant.trigger.requestCheckpoint(at(1));
        long store = waitFor(() -> stored().contains(1L), 5_000);
        print("P0-control", "storeMs=" + store + " stored=" + stored() + " advanced=" + participant.advanced);
    }

    /** Steven's point 1: an agreement that never completes. */
    @Test
    void agreementThatNeverCompletes() throws Exception {
        ProbeParticipant participant = new ProbeParticipant();
        participant.advance = requested -> requested.position().orElse(0) == 5
                ? new CompletableFuture<>()
                : CompletableFuture.completedFuture(requested);
        WorkPackageHarness subject = deferred(participant);
        subject.onSegmentClaimed();
        for (long i = 1; i <= 5; i++) {
            subject.scheduleEvent(eventAt(i));
        }
        waitFor(() -> subject.batchProcessor().processed().size() == 5, 5_000);
        long start = System.nanoTime();
        participant.trigger.requestCheckpoint(at(5));
        for (long i = 6; i <= 9; i++) {
            Thread.sleep(200);
            subject.scheduleEvent(eventAt(i));
            if (i == 7 || i == 9) {
                participant.trigger.requestCheckpoint(at(i));
            }
        }
        Thread.sleep(34_000);
        subject.scheduleEvent(eventAt(10));
        long event10 = waitFor(() -> subject.batchProcessor().processed().size() >= 10, 2_000);
        print("P2-never", "afterMs=" + ms(start) + " handled=" + subject.batchProcessor().processed().size()
                + " event10HandedMs=" + event10 + " stored=" + stored() + " advanced=" + participant.advanced);
    }

    /** Steven's point 2: an agreement that fails after the worker moved on. */
    @Test
    void agreementThatFailsLater() throws Exception {
        ProbeParticipant participant = new ProbeParticipant();
        participant.advance = requested -> {
            if (requested.position().orElse(0) == 5) {
                CompletableFuture<TrackingToken> failing = new CompletableFuture<>();
                side.schedule(() -> failing.completeExceptionally(new IllegalStateException("durable write failed")),
                              300, TimeUnit.MILLISECONDS);
                return failing;
            }
            return CompletableFuture.completedFuture(requested);
        };
        WorkPackageHarness subject = deferred(participant);
        subject.onSegmentClaimed();
        for (long i = 1; i <= 5; i++) {
            subject.scheduleEvent(eventAt(i));
        }
        waitFor(() -> subject.batchProcessor().processed().size() == 5, 5_000);
        long start = System.nanoTime();
        participant.trigger.requestCheckpoint(at(5));
        Thread.sleep(1_000);
        subject.scheduleEvent(eventAt(6));
        subject.scheduleEvent(eventAt(7));
        waitFor(() -> subject.batchProcessor().processed().size() >= 7, 2_000);
        participant.trigger.requestCheckpoint(at(7));
        long store7 = waitFor(() -> stored().contains(7L), 2_000);
        print("P3-fails", "handled=" + subject.batchProcessor().processed().size() + " store7Ms=" + store7
                + " stored=" + stored() + " advanced=" + participant.advanced + " afterMs=" + ms(start));
    }

    /** Control for Steven's point 2: an agreement that fails at once (already failed when returned). */
    @Test
    void agreementThatFailsAtOnce() throws Exception {
        ProbeParticipant participant = new ProbeParticipant();
        participant.advance = requested -> requested.position().orElse(0) == 5
                ? CompletableFuture.failedFuture(new IllegalStateException("durable write failed"))
                : CompletableFuture.completedFuture(requested);
        WorkPackageHarness subject = deferred(participant);
        subject.onSegmentClaimed();
        for (long i = 1; i <= 5; i++) {
            subject.scheduleEvent(eventAt(i));
        }
        waitFor(() -> subject.batchProcessor().processed().size() == 5, 5_000);
        participant.trigger.requestCheckpoint(at(5));
        Thread.sleep(500);
        subject.scheduleEvent(eventAt(6));
        long event6 = waitFor(() -> subject.batchProcessor().processed().size() >= 6, 2_000);
        print("P3b-failsAtOnce", "event6HandedMs=" + event6 + " stored=" + stored()
                + " advanced=" + participant.advanced);
    }

    /** Release while an agreement is awaited; the agreement lands after the release. */
    @Test
    void releaseWhileAgreementIsAwaited() throws Exception {
        ProbeParticipant participant = new ProbeParticipant();
        CompletableFuture<TrackingToken> safe = new CompletableFuture<>();
        participant.advance = requested -> safe;
        WorkPackageHarness subject = deferred(participant);
        subject.onSegmentClaimed();
        for (long i = 1; i <= 5; i++) {
            subject.scheduleEvent(eventAt(i));
        }
        waitFor(() -> subject.batchProcessor().processed().size() == 5, 5_000);
        participant.trigger.requestCheckpoint(at(5));
        waitFor(() -> participant.advanced.size() == 1, 2_000);
        Thread.sleep(50);

        long start = System.nanoTime();
        CompletableFuture<Throwable> aborted = subject.abort(null);
        long abortMs = waitFor(aborted::isDone, 40_000);
        long releaseStart = System.nanoTime();
        subject.release().get(10, TimeUnit.SECONDS);
        long releaseMs = ms(releaseStart);
        List<Long> storedAtRelease = stored();
        boolean pendingAtRelease = !safe.isDone();
        safe.complete(at(5));
        Thread.sleep(500);
        print("P4-release", "abortDoneMs=" + abortMs + " releaseMs=" + releaseMs
                + " advancePendingWhenReleaseCalled=" + participant.releaseWhileAdvancePending.get()
                + " safePendingAfterRelease=" + pendingAtRelease
                + " storedAtRelease=" + storedAtRelease + " storedAfterLateAgreement=" + stored()
                + " totalMs=" + ms(start));
    }

    /**
     * The release drain waits on the same work as the awaited agreement, which completes 6 s after the abort. Shows
     * where the wait lands: in the worker (before the release sequence) or in the final drain (which AF PR 5318 caps
     * at claimExtensionThreshold).
     */
    @Test
    void releaseDrainWaitsOnTheSameWork() throws Exception {
        ProbeParticipant participant = new ProbeParticipant();
        CompletableFuture<TrackingToken> safe = new CompletableFuture<>();
        participant.advance = requested -> safe;
        participant.release = upTo -> safe.thenApply(ignored -> upTo);
        WorkPackageHarness subject = deferred(participant);
        subject.onSegmentClaimed();
        for (long i = 1; i <= 5; i++) {
            subject.scheduleEvent(eventAt(i));
        }
        waitFor(() -> subject.batchProcessor().processed().size() == 5, 5_000);
        participant.trigger.requestCheckpoint(at(5));
        waitFor(() -> participant.advanced.size() == 1, 2_000);
        Thread.sleep(50);

        long start = System.nanoTime();
        side.schedule(() -> safe.complete(at(5)), 6, TimeUnit.SECONDS);
        CompletableFuture<Throwable> aborted = subject.abort(null);
        long abortMs = waitFor(aborted::isDone, 40_000);
        long releaseStart = System.nanoTime();
        subject.release().get(40, TimeUnit.SECONDS);
        long releaseMs = ms(releaseStart);
        print("P5-drain", "abortDoneMs=" + abortMs + " finalDrainMs=" + releaseMs + " stored=" + stored()
                + " totalMs=" + ms(start));
    }

    /**
     * Throughput and lost requests: every batch requests a checkpoint at its batch end; each agreement completes on
     * another thread after a random 0 to {@code delayMs} ms. 200 events.
     */
    @Test
    void busyStreamWithSlowAgreements() throws Exception {
        for (int delayMs : new int[]{0, 2, 20}) {
            ProbeParticipant participant = new ProbeParticipant();
            participant.advance = requested -> {
                CompletableFuture<TrackingToken> done = new CompletableFuture<>();
                long delay = delayMs == 0 ? 0 : ThreadLocalRandom.current().nextLong(delayMs + 1);
                side.schedule(() -> done.complete(requested), delay, TimeUnit.MILLISECONDS);
                return done;
            };
            WorkPackageHarness subject = deferred(participant);
            AtomicLong lastBatch = new AtomicLong(System.nanoTime());
            AtomicLong maxGap = new AtomicLong();
            subject.batchProcessor().observeContext(ctx -> {
                long now = System.nanoTime();
                maxGap.accumulateAndGet(now - lastBatch.getAndSet(now), Math::max);
                TrackingToken end = ctx.getResource(TrackingToken.BATCH_END_RESOURCE_KEY);
                participant.trigger.requestCheckpoint(end);
            });
            subject.onSegmentClaimed();
            long start = System.nanoTime();
            lastBatch.set(start);
            for (long i = 1; i <= 200; i++) {
                subject.scheduleEvent(eventAt(i));
            }
            long handled = waitFor(() -> subject.batchProcessor().processed().size() == 200, 60_000);
            long final200 = waitFor(() -> stored().contains(200L), 5_000);
            List<Long> stores = stored();
            boolean monotonic = true;
            for (int i = 1; i < stores.size(); i++) {
                monotonic &= stores.get(i) > stores.get(i - 1);
            }
            print("P6-busy", "delayMs=0.." + delayMs + " all200HandledMs=" + handled
                    + " maxGapBetweenBatchesMs=" + TimeUnit.NANOSECONDS.toMillis(maxGap.get())
                    + " stores=" + stores.size() + " last=" + (stores.isEmpty() ? null : stores.getLast())
                    + " final200StoredAfterMs=" + final200 + " monotonic=" + monotonic
                    + " advanceCalls=" + participant.advanced.size()
                    + " maxConcurrentAdvances=" + participant.maxInFlight.get());
            subject.abort(null).get(40, TimeUnit.SECONDS);
            org.mockito.Mockito.clearInvocations(tokenStore());
        }
    }

    static class ProbeParticipant implements Checkpointing {

        final List<TrackingToken> advanced = new CopyOnWriteArrayList<>();
        final AtomicInteger inFlight = new AtomicInteger();
        final AtomicInteger maxInFlight = new AtomicInteger();
        final AtomicBoolean releaseWhileAdvancePending = new AtomicBoolean();
        volatile CheckpointTrigger trigger;
        volatile Function<TrackingToken, CompletableFuture<TrackingToken>> advance =
                CompletableFuture::completedFuture;
        volatile Function<TrackingToken, CompletableFuture<TrackingToken>> release =
                CompletableFuture::completedFuture;

        @Override
        public void onSegmentClaimed(Segment segment, @Nullable TrackingToken from, CheckpointTrigger trigger) {
            this.trigger = trigger;
        }

        @Override
        public CompletableFuture<TrackingToken> onCheckpointAdvanced(Segment segment, TrackingToken requested) {
            advanced.add(requested);
            maxInFlight.accumulateAndGet(inFlight.incrementAndGet(), Math::max);
            return advance.apply(requested).whenComplete((r, e) -> inFlight.decrementAndGet());
        }

        @Override
        public CompletableFuture<TrackingToken> onSegmentReleased(Segment segment, TrackingToken upTo) {
            releaseWhileAdvancePending.set(inFlight.get() > 0);
            return release.apply(upTo);
        }
    }
}
