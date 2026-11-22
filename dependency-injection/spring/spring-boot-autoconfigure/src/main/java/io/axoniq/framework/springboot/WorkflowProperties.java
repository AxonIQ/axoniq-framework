/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.springboot;

import org.jspecify.annotations.Nullable;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * The workflow properties exposed to Spring Boot applications under the {@code axon.workflow} prefix.
 * <p>
 * Every property mirrors a setting Axon's own {@code EventProcessorProperties.ProcessorSettings} exposes per named
 * processor, without the per-processor-name split: these top-level properties apply to every workflow module's engine
 * processor uniformly. Each defaults to the exact same value {@code ProcessorSettings} itself defaults to, so an
 * application that sets none of them gets identical behavior to a plain {@code PooledStreamingEventProcessor}.
 * <p>
 * The history projector runs in its own, separate event processor - see {@link #getHistory()} for the equivalent
 * properties that apply to that processor instead.
 *
 * @author Stefan Dragisic
 * @author Steven van Beelen
 * @since 5.4.0
 */
@ConfigurationProperties(prefix = "axon.workflow")
public class WorkflowProperties {

    /**
     * The number of segments to initialize the workflow event processor with. Workflow instances are partitioned over
     * segments by workflow id, so this decides how far the instances of one application can spread over its nodes.
     * <p>
     * Defaults to {@code 16}, matching {@code EventProcessorProperties.ProcessorSettings}. Only the initial count is
     * set here: once the processor has stored its tokens, the segment count changes through splitting and merging
     * them.
     * <p>
     * Setting this above 1 only spreads instances across nodes when the application also registers a durable
     * {@code TokenStore}. Without one, the processor falls back to an in-memory token store and every claim stays
     * process-local, so multiple segments run on the same node without actually distributing any load.
     */
    private int initialSegmentCount = 16;

    /**
     * The maximum number of events read from the event store per source poll.
     * <p>
     * Defaults to {@code 1}, matching {@code EventProcessorProperties.ProcessorSettings}.
     */
    private int batchSize = 1;

    /**
     * The number of threads the workflow event processor uses to process events concurrently.
     * <p>
     * Defaults to {@code 4}, matching {@code EventProcessorProperties.ProcessorSettings}.
     */
    private int threadCount = 4;

    /**
     * The interval, in milliseconds, at which the workflow event processor's coordinator attempts to claim unclaimed
     * segments.
     * <p>
     * Defaults to {@code 5000}, matching {@code EventProcessorProperties.ProcessorSettings}.
     */
    private long tokenClaimInterval = 5000L;

    /**
     * The duration, in milliseconds, after which the workflow event processor extends a segment claim to keep other
     * nodes from stealing it while this node is still working it.
     * <p>
     * Defaults to {@code 5000}, matching {@code EventProcessorProperties.ProcessorSettings}.
     */
    private long claimExtensionThreshold = 5000L;

    /**
     * Whether the workflow event processor's coordinator - not only its workers - also extends segment claims.
     * <p>
     * Defaults to {@code false}, matching {@code EventProcessorProperties.ProcessorSettings}. There is no explicit
     * "disable" - an application that wants to force this off should do so through the generic event processing
     * configuration instead.
     */
    private boolean coordinatorClaimExtension = false;

    /**
     * The minimum time, in milliseconds, between two checkpoint requests of one segment of the workflow event
     * processor.
     * <p>
     * A request within the interval is dropped, so the stored position lags by up to one interval, and the events of
     * that interval are processed again after a restart. A segment that goes idle keeps that lag until its next event,
     * or until the processor releases it. Defaults to {@code 0}, which requests a checkpoint after every event.
     */
    private long checkpointInterval = 0L;

    /**
     * The index of running workflow instances the workflow engine keeps, so that claiming a segment does not read every
     * workflow lifecycle event ever written.
     * <p>
     * Defaults to {@link WorkflowIndexType#NONE}: every claim reads the running workflow instances from the event
     * store. {@link WorkflowIndexType#JPA} keeps the index in the application's database, which requires an
     * {@code EntityManagerFactory} bean. A {@code WorkflowIndex} bean the application defines is used whatever this
     * property says.
     */
    private WorkflowIndexType index = WorkflowIndexType.NONE;
    private final HistoryProcessorProperties history = new HistoryProcessorProperties();
    private final SnapshotProperties snapshots = new SnapshotProperties();
    private final HibernationProperties hibernation = new HibernationProperties();

    /**
     * Returns the number of segments to initialize the workflow event processor with.
     *
     * @return the number of segments to initialize the workflow event processor with
     */
    public int getInitialSegmentCount() {
        return initialSegmentCount;
    }

    /**
     * Sets the number of segments to initialize the workflow event processor with.
     *
     * @param initialSegmentCount the number of segments to initialize the workflow event processor with
     */
    public void setInitialSegmentCount(int initialSegmentCount) {
        this.initialSegmentCount = initialSegmentCount;
    }

    /**
     * Returns the maximum number of events read from the event store per source poll.
     *
     * @return the maximum number of events read from the event store per source poll
     */
    public int getBatchSize() {
        return batchSize;
    }

    /**
     * Sets the maximum number of events read from the event store per source poll.
     *
     * @param batchSize the maximum number of events read from the event store per source poll
     */
    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    /**
     * Returns the number of threads the workflow event processor uses to process events concurrently.
     *
     * @return the number of threads the workflow event processor uses to process events concurrently
     */
    public int getThreadCount() {
        return threadCount;
    }

    /**
     * Sets the number of threads the workflow event processor uses to process events concurrently.
     *
     * @param threadCount the number of threads the workflow event processor uses to process events concurrently
     */
    public void setThreadCount(int threadCount) {
        this.threadCount = threadCount;
    }

    /**
     * Returns the interval, in milliseconds, at which the workflow event processor's coordinator attempts to claim
     * unclaimed segments.
     *
     * @return the segment-claim interval, in milliseconds
     */
    public long getTokenClaimInterval() {
        return tokenClaimInterval;
    }

    /**
     * Sets the interval, in milliseconds, at which the workflow event processor's coordinator attempts to claim
     * unclaimed segments.
     *
     * @param tokenClaimInterval the segment-claim interval, in milliseconds
     */
    public void setTokenClaimInterval(long tokenClaimInterval) {
        this.tokenClaimInterval = tokenClaimInterval;
    }

    /**
     * Returns the duration, in milliseconds, after which the workflow event processor extends a segment claim.
     *
     * @return the claim extension threshold, in milliseconds
     */
    public long getClaimExtensionThreshold() {
        return claimExtensionThreshold;
    }

    /**
     * Sets the duration, in milliseconds, after which the workflow event processor extends a segment claim.
     *
     * @param claimExtensionThreshold the claim extension threshold, in milliseconds
     */
    public void setClaimExtensionThreshold(long claimExtensionThreshold) {
        this.claimExtensionThreshold = claimExtensionThreshold;
    }

    /**
     * Returns whether the workflow event processor's coordinator also extends segment claims.
     *
     * @return whether the workflow event processor's coordinator also extends segment claims
     */
    public boolean getCoordinatorClaimExtension() {
        return coordinatorClaimExtension;
    }

    /**
     * Sets whether the workflow event processor's coordinator also extends segment claims.
     *
     * @param coordinatorClaimExtension whether the workflow event processor's coordinator also extends segment claims
     */
    public void setCoordinatorClaimExtension(boolean coordinatorClaimExtension) {
        this.coordinatorClaimExtension = coordinatorClaimExtension;
    }

    /**
     * Returns the minimum time, in milliseconds, between two checkpoint requests of one segment of the workflow event
     * processor.
     * <p>
     * A request within the interval is dropped, so the stored position lags by up to one interval, and the events of
     * that interval are processed again after a restart. A segment that goes idle keeps that lag until its next event,
     * or until the processor releases it. Defaults to {@code 0}, which requests a checkpoint after every event.
     *
     * @return the checkpoint interval, in milliseconds, where {@code 0} requests a checkpoint after every event
     */
    public long getCheckpointInterval() {
        return checkpointInterval;
    }

    /**
     * Sets the minimum time, in milliseconds, between two checkpoint requests of one segment of the workflow event
     * processor.
     * <p>
     * A request within the interval is dropped, so the stored position lags by up to one interval, and the events of
     * that interval are processed again after a restart. A segment that goes idle keeps that lag until its next event,
     * or until the processor releases it. Defaults to {@code 0}, which requests a checkpoint after every event.
     *
     * @param checkpointInterval the checkpoint interval, in milliseconds, where {@code 0} requests a checkpoint after
     *                           every event
     */
    public void setCheckpointInterval(long checkpointInterval) {
        this.checkpointInterval = checkpointInterval;
    }

    /**
     * The properties of the history projector's own, dedicated event processor, under the
     * {@code axoniq.workflow.history} prefix.
     * <p>
     * The history projector runs in a separate event processor from the workflow engine's own, so it can be given a
     * different (e.g. durable) token store and tuned independently - for example to make history projections durable
     * without also changing how the engine's own processor behaves.
     *
     * @return the history projector's own processor properties
     */
    public HistoryProcessorProperties getHistory() {
        return history;
    }

    /**
     * Returns the index of running workflow instances the workflow engine keeps.
     *
     * @return the type of workflow index, {@link WorkflowIndexType#NONE} unless configured otherwise
     */
    public WorkflowIndexType getIndex() {
        return index;
    }

    /**
     * Sets the index of running workflow instances the workflow engine keeps.
     *
     * @param index the type of workflow index
     */
    public void setIndex(WorkflowIndexType index) {
        this.index = index;
    }

    /**
     * The snapshot properties of the workflow engine, under the {@code axon.workflow.snapshots} prefix.
     *
     * @return the snapshot properties of the workflow engine
     */
    public SnapshotProperties getSnapshots() {
        return snapshots;
    }

    /**
     * The hibernation properties of the workflow engine, under the {@code axon.workflow.hibernation} prefix.
     *
     * @return the hibernation properties of the workflow engine
     * @since 5.5.0
     */
    public HibernationProperties getHibernation() {
        return hibernation;
    }

    /**
     * The types of index of running workflow instances the workflow engine can keep, selected through the
     * {@code axon.workflow.index} property.
     */
    public enum WorkflowIndexType {

        /**
         * Keeps no index: every claim of a segment reads the running workflow instances from the event store.
         */
        NONE,
        /**
         * Keeps the index in the application's database through a
         * {@link io.axoniq.framework.workflow.index.jpa.JpaWorkflowIndex}.
         * <p>
         * Enable it only once every node that runs the workflow modules uses it: a node without the index advances the
         * tokens without writing the index. See the reference guide for introducing the index into a running
         * application.
         */
        JPA
    }

    /**
     * The snapshot properties of the workflow engine.
     * <p>
     * When {@code enabled} is not set, the state of each workflow instance is snapshotted whenever the application can
     * take snapshots: it has a {@code SnapshotStore} bean, a {@code GeneralConverter}, and a
     * {@code MessageTypeResolver} that resolves the type of the workflow state. The running-workflow list is then not
     * snapshotted. Setting {@code enabled} to {@code true} snapshots both, and requires a {@code SnapshotStore} bean;
     * setting it to {@code false} takes no snapshot at all.
     */
    public static class SnapshotProperties {

        @Nullable
        private Boolean enabled;
        private final SnapshotPolicyProperties runningWorkflows = new SnapshotPolicyProperties();
        private final SnapshotPolicyProperties workflowState = new SnapshotPolicyProperties();

        /**
         * Returns whether snapshots are explicitly enabled or disabled.
         *
         * @return {@code true} when snapshots are enabled, {@code false} when they are disabled, or {@code null} when
         * the state of each workflow instance is snapshotted whenever the application can take snapshots
         */
        @Nullable
        public Boolean getEnabled() {
            return enabled;
        }

        /**
         * Sets whether snapshots are explicitly enabled or disabled.
         *
         * @param enabled {@code true} to enable snapshots, {@code false} to disable them, or {@code null} to snapshot
         *                the state of each workflow instance whenever the application can take snapshots
         */
        public void setEnabled(@Nullable Boolean enabled) {
            this.enabled = enabled;
        }

        /**
         * Sets whether snapshots are explicitly enabled or disabled, like {@link #setEnabled(Boolean)} does with the
         * same value.
         *
         * @param enabled {@code true} to enable snapshots, {@code false} to disable them
         */
        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        /**
         * Returns whether snapshots are explicitly enabled.
         * <p>
         * Up to 5.4, snapshots were taken only when enabled, so this method told whether snapshots were taken. Since
         * 5.5.0, a {@code false} result no longer means that no snapshot is taken: unless snapshots are explicitly
         * disabled, the state of each workflow instance is snapshotted whenever the application can take snapshots.
         *
         * @return {@code true} when snapshots are explicitly enabled, {@code false} otherwise
         * @deprecated use {@link #getEnabled()}, which also tells whether snapshots are explicitly disabled
         */
        @Deprecated(since = "5.5.0")
        public boolean isEnabled() {
            return Boolean.TRUE.equals(enabled);
        }

        /**
         * The snapshot properties of the running-workflow list, under the
         * {@code axon.workflow.snapshots.running-workflows} prefix.
         *
         * @return the snapshot properties of the running-workflow list
         */
        public SnapshotPolicyProperties getRunningWorkflows() {
            return runningWorkflows;
        }

        /**
         * The snapshot properties of the state of each workflow instance, under the
         * {@code axon.workflow.snapshots.workflow-state} prefix.
         *
         * @return the snapshot properties of the state of each workflow instance
         */
        public SnapshotPolicyProperties getWorkflowState() {
            return workflowState;
        }
    }

    /**
     * The properties deciding when a snapshot is taken.
     */
    public static class SnapshotPolicyProperties {

        private int afterEvents = 50;

        /**
         * Returns the number of sourced events after which a snapshot is taken.
         *
         * @return the number of sourced events after which a snapshot is taken
         */
        public int getAfterEvents() {
            return afterEvents;
        }

        /**
         * Sets the number of sourced events after which a snapshot is taken.
         *
         * @param afterEvents the number of sourced events after which a snapshot is taken
         */
        public void setAfterEvents(int afterEvents) {
            this.afterEvents = afterEvents;
        }
    }

    /**
     * The properties of the history projector's own, dedicated event processor.
     * <p>
     * Mirrors the same settings {@link WorkflowProperties} exposes for the workflow engine's own processor - see there
     * for what each property means and what it defaults to.
     */
    public static class HistoryProcessorProperties {

        private int initialSegmentCount = 16;
        private int batchSize = 1;
        private int threadCount = 4;
        private long tokenClaimInterval = 5000L;
        private long claimExtensionThreshold = 5000L;
        private boolean coordinatorClaimExtension = false;

        /**
         * Returns the number of segments to initialize the history projector's event processor with.
         *
         * @return the number of segments to initialize the history projector's event processor with
         */
        public int getInitialSegmentCount() {
            return initialSegmentCount;
        }

        /**
         * Sets the number of segments to initialize the history projector's event processor with.
         *
         * @param initialSegmentCount the number of segments to initialize the history projector's event processor with
         */
        public void setInitialSegmentCount(int initialSegmentCount) {
            this.initialSegmentCount = initialSegmentCount;
        }

        /**
         * Returns the maximum number of events read from the event store per source poll.
         *
         * @return the maximum number of events read from the event store per source poll
         */
        public int getBatchSize() {
            return batchSize;
        }

        /**
         * Sets the maximum number of events read from the event store per source poll.
         *
         * @param batchSize the maximum number of events read from the event store per source poll
         */
        public void setBatchSize(int batchSize) {
            this.batchSize = batchSize;
        }

        /**
         * Returns the number of threads the history processor uses to process events concurrently.
         *
         * @return the number of threads the history processor uses to process events concurrently
         */
        public int getThreadCount() {
            return threadCount;
        }

        /**
         * Sets the number of threads the history processor uses to process events concurrently.
         *
         * @param threadCount the number of threads the history processor uses to process events concurrently
         */
        public void setThreadCount(int threadCount) {
            this.threadCount = threadCount;
        }

        /**
         * Returns the interval, in milliseconds, at which the history processor's coordinator attempts to claim
         * unclaimed segments.
         *
         * @return the segment-claim interval, in milliseconds
         */
        public long getTokenClaimInterval() {
            return tokenClaimInterval;
        }

        /**
         * Sets the interval, in milliseconds, at which the history processor's coordinator attempts to claim unclaimed
         * segments.
         *
         * @param tokenClaimInterval the segment-claim interval, in milliseconds
         */
        public void setTokenClaimInterval(long tokenClaimInterval) {
            this.tokenClaimInterval = tokenClaimInterval;
        }

        /**
         * Returns the duration, in milliseconds, after which the history processor extends a segment claim.
         *
         * @return the claim extension threshold, in milliseconds
         */
        public long getClaimExtensionThreshold() {
            return claimExtensionThreshold;
        }

        /**
         * Sets the duration, in milliseconds, after which the history processor extends a segment claim.
         *
         * @param claimExtensionThreshold the claim extension threshold, in milliseconds
         */
        public void setClaimExtensionThreshold(long claimExtensionThreshold) {
            this.claimExtensionThreshold = claimExtensionThreshold;
        }

        /**
         * Returns whether the history processor's coordinator also extends segment claims.
         *
         * @return whether the history processor's coordinator also extends segment claims
         */
        public boolean getCoordinatorClaimExtension() {
            return coordinatorClaimExtension;
        }

        /**
         * Sets whether the history processor's coordinator also extends segment claims.
         *
         * @param coordinatorClaimExtension whether the history processor's coordinator also extends segment claims
         */
        public void setCoordinatorClaimExtension(boolean coordinatorClaimExtension) {
            this.coordinatorClaimExtension = coordinatorClaimExtension;
        }
    }

    /**
     * The hibernation properties of the workflow engine, under the {@code axon.workflow.hibernation} prefix.
     * <p>
     * A hibernated workflow is removed from memory while it only waits for an external event or a deadline, and
     * restored from the event store when that event arrives, that deadline expires, or a cancellation is requested for
     * it. Hibernation is disabled by default.
     *
     * @since 5.5.0
     */
    public static class HibernationProperties {

        /**
         * Whether waiting workflows hibernate. Defaults to {@code false}.
         */
        private boolean enabled = false;
        /**
         * The time a workflow must make no progress before it hibernates. Defaults to 30 seconds.
         */
        private Duration idleDelay = Duration.ofSeconds(30);
        /**
         * The time a wake may take to load a hibernated workflow from the event store. Defaults to 10 seconds.
         */
        private Duration wakeTimeout = Duration.ofSeconds(10);

        /**
         * Returns whether waiting workflows hibernate.
         *
         * @return {@code true} when waiting workflows hibernate
         */
        public boolean isEnabled() {
            return enabled;
        }

        /**
         * Sets whether waiting workflows hibernate.
         *
         * @param enabled whether waiting workflows hibernate
         */
        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        /**
         * Returns the time a workflow must make no progress before it hibernates.
         *
         * @return the idle delay
         */
        public Duration getIdleDelay() {
            return idleDelay;
        }

        /**
         * Sets the time a workflow must make no progress before it hibernates.
         *
         * @param idleDelay the idle delay
         */
        public void setIdleDelay(Duration idleDelay) {
            this.idleDelay = idleDelay;
        }

        /**
         * Returns the time a wake may take to load a hibernated workflow from the event store.
         *
         * @return the wake timeout
         */
        public Duration getWakeTimeout() {
            return wakeTimeout;
        }

        /**
         * Sets the time a wake may take to load a hibernated workflow from the event store.
         *
         * @param wakeTimeout the wake timeout
         */
        public void setWakeTimeout(Duration wakeTimeout) {
            this.wakeTimeout = wakeTimeout;
        }
    }
}
