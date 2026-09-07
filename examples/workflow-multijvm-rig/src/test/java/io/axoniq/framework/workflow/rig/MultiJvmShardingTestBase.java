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
package io.axoniq.framework.workflow.rig;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Base class for every sharding and failover scenario in this repository.
 * <p>
 * Extending it is the only supported way to get nodes, and {@link #cluster(int, int)} always forks real processes. A
 * scenario therefore cannot accidentally be written in-JVM, where identical {@code nodeId}s, a shared
 * {@code ClockUtils} clock and claim-releasing shutdown hooks would make every ownership assertion pass vacuously.
 * <p>
 * On failure the extension below prints every node's captured output, so a broken run is diagnosable from the build
 * log alone.
 */
public abstract class MultiJvmShardingTestBase {

    private ShardCluster cluster;

    @RegisterExtension
    final AfterTestExecutionCallback dumpNodeLogsOnFailure = context -> {
        if (context.getExecutionException().isPresent() && cluster != null) {
            System.out.println(cluster.dumpNodeLogs());
        }
    };

    /**
     * Creates the scenario's cluster. One per test; the previous one is torn down automatically.
     *
     * @param segmentCount              segments the workflow processor is initialized with.
     * @param maxClaimedSegmentsPerNode cap per node, so segments actually split over the nodes.
     * @return the cluster.
     */
    protected final ShardCluster cluster(int segmentCount, int maxClaimedSegmentsPerNode) {
        cluster = new ShardCluster(segmentCount, maxClaimedSegmentsPerNode);
        return cluster;
    }

    @AfterEach
    final void stopCluster() {
        if (cluster != null) {
            cluster.assertGenuinelyDistributed();
            cluster.close();
            cluster = null;
        }
    }
}
