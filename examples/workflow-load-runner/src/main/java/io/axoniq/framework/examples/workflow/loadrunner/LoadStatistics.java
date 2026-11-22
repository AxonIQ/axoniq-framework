/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.examples.workflow.loadrunner;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Counts what the load runner and its workflow instances did since the application started.
 *
 * @author Allard Buijze
 */
public final class LoadStatistics {

    private final String runId;
    private final AtomicLong published = new AtomicLong();
    private final AtomicLong startedThisRun = new AtomicLong();
    private final AtomicLong approvalsPublished = new AtomicLong();
    private final AtomicLong completed = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();
    private final Map<String, Long> approvedAt = new ConcurrentHashMap<>();
    private final Collection<Long> approvalLatenciesMillis = new ConcurrentLinkedQueue<>();

    /**
     * Creates statistics for the run with the given id.
     *
     * @param runId the id of this run, which prefixes the workflow ids of the instances this run starts
     */
    LoadStatistics(String runId) {
        this.runId = runId;
    }

    void recordPublished(int count) {
        published.addAndGet(count);
    }

    void recordStarted(String workflowId) {
        if (workflowId.startsWith(runId)) {
            startedThisRun.incrementAndGet();
        }
    }

    void recordApprovalsPublishing(Collection<String> workflowIds) {
        long now = System.nanoTime();
        workflowIds.forEach(workflowId -> approvedAt.put(workflowId, now));
    }

    void recordApprovals(int count) {
        approvalsPublished.addAndGet(count);
    }

    void recordCompleted(String workflowId) {
        completed.incrementAndGet();
        Long approved = approvedAt.remove(workflowId);
        if (approved != null) {
            approvalLatenciesMillis.add(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - approved));
        }
    }

    void recordFailed() {
        failed.incrementAndGet();
    }

    /**
     * Returns the number of start events this run published.
     *
     * @return the number of start events published
     */
    public long published() {
        return published.get();
    }

    /**
     * Returns the number of instances this run started that reached the {@code STARTED} status.
     *
     * @return the number of instances of this run that started
     */
    public long startedThisRun() {
        return startedThisRun.get();
    }

    /**
     * Returns the number of approval events published.
     *
     * @return the number of approval events published
     */
    public long approvalsPublished() {
        return approvalsPublished.get();
    }

    /**
     * Returns the number of instances that completed, approved or timed out, including restored ones.
     *
     * @return the number of completed instances
     */
    public long completed() {
        return completed.get();
    }

    /**
     * Describes the time from publishing an approval until the approved instance completed, over the approved
     * instances that completed in this process: their count, and the median, 95th percentile and maximum in
     * milliseconds. When the approved instance was hibernated, this includes waking it.
     *
     * @return a summary of the approval latencies, or {@code "none"} when no approved instance completed yet
     */
    public String approvalLatencies() {
        List<Long> sorted = new ArrayList<>(approvalLatenciesMillis);
        if (sorted.isEmpty()) {
            return "none";
        }
        sorted.sort(null);
        return "n=" + sorted.size()
                + ", p50 " + sorted.get(sorted.size() / 2)
                + " ms, p95 " + sorted.get((int) Math.min(sorted.size() - 1, Math.round(sorted.size() * 0.95)))
                + " ms, max " + sorted.getLast() + " ms";
    }

    /**
     * Returns the number of instances that failed.
     *
     * @return the number of failed instances
     */
    public long failed() {
        return failed.get();
    }
}
