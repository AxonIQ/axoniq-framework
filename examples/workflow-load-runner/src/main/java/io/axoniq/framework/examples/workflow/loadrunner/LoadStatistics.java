/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.examples.workflow.loadrunner;

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

    void recordApprovals(int count) {
        approvalsPublished.addAndGet(count);
    }

    void recordCompleted() {
        completed.incrementAndGet();
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
     * Returns the number of instances that failed.
     *
     * @return the number of failed instances
     */
    public long failed() {
        return failed.get();
    }
}
