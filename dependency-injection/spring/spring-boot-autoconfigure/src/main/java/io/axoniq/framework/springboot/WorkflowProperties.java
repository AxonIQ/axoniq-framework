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
package io.axoniq.framework.springboot;

import org.springframework.boot.context.properties.ConfigurationProperties;

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
    private final HistoryProcessorProperties history = new HistoryProcessorProperties();

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
}
