/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */

package org.axonframework.messaging.eventsourcing;

import org.axonframework.messaging.eventsourcing.snapshotting.DefaultSnapshotterSpanFactory;
import org.axonframework.messaging.tracing.IntermediateSpanFactoryTest;
import org.axonframework.messaging.tracing.SpanFactory;
import org.axonframework.messaging.tracing.TestSpanFactory;
import org.junit.jupiter.api.*;


class DefaultSnapshotterSpanFactoryTest
        extends IntermediateSpanFactoryTest<DefaultSnapshotterSpanFactory.Builder, DefaultSnapshotterSpanFactory> {

    @Test
    void createScheduleSnapshotSpanWithDefaults() {
        test(spanFactory -> spanFactory.createScheduleSnapshotSpan("MyAggregateType", "3728973982"),
             expectedSpan("Snapshotter.scheduleSnapshot(MyAggregateType)", TestSpanFactory.TestSpanType.INTERNAL)
                     .expectAttribute("aggregateIdentifier", "3728973982")
        );
    }

    @Test
    void scheduleSnapshotSpanIncludesAggregateName() {
        test(builder -> builder.aggregateTypeInSpanName(true).separateTrace(false),
             spanFactory -> spanFactory.createScheduleSnapshotSpan("MyAggregateType", "3728973982"),
             expectedSpan("Snapshotter.scheduleSnapshot(MyAggregateType)", TestSpanFactory.TestSpanType.INTERNAL)
                     .expectAttribute("aggregateIdentifier", "3728973982")
        );
    }

    @Test
    void scheduleSnapshotSpanDoesntIncludeAggregateName() {
        test(builder -> builder.aggregateTypeInSpanName(false).separateTrace(false),
             spanFactory -> spanFactory.createScheduleSnapshotSpan("MyAggregateType", "3728973982"),
             expectedSpan("Snapshotter.scheduleSnapshot", TestSpanFactory.TestSpanType.INTERNAL)
                     .expectAttribute("aggregateIdentifier", "3728973982")
        );
    }

    @Test
    void scheduleSnapshotSpanIsNotAffectedBySeparateTrace() {
        test(builder -> builder.aggregateTypeInSpanName(false).separateTrace(true),
             spanFactory -> spanFactory.createScheduleSnapshotSpan("MyAggregateType", "3728973982"),
             expectedSpan("Snapshotter.scheduleSnapshot", TestSpanFactory.TestSpanType.INTERNAL)
                     .expectAttribute("aggregateIdentifier", "3728973982")
        );
    }

    @Test
    void createSnapshotSpanWithDefaults() {
        test(spanFactory -> spanFactory.createCreateSnapshotSpan("MyAggregateType", "3728973982"),
             expectedSpan("Snapshotter.createSnapshot(MyAggregateType)", TestSpanFactory.TestSpanType.INTERNAL)
                     .expectAttribute("aggregateIdentifier", "3728973982")
        );
    }

    @Test
    void createSnapshotSpanWithSeparateTraceAndWithoutAggregateInSpanName() {
        test(builder -> builder.aggregateTypeInSpanName(false).separateTrace(true),
             spanFactory -> spanFactory.createCreateSnapshotSpan("MyAggregateType", "3728973982"),
             expectedSpan("Snapshotter.createSnapshot", TestSpanFactory.TestSpanType.ROOT)
                     .expectAttribute("aggregateIdentifier", "3728973982")
        );
    }

    @Test
    void createSnapshotSpanWithInnerTraceAndWithoutAggregateInSpanName() {
        test(builder -> builder.aggregateTypeInSpanName(false).separateTrace(false),
             spanFactory -> spanFactory.createCreateSnapshotSpan("MyAggregateType", "3728973982"),
             expectedSpan("Snapshotter.createSnapshot", TestSpanFactory.TestSpanType.INTERNAL)
                     .expectAttribute("aggregateIdentifier", "3728973982")
        );
    }

    @Override
    protected DefaultSnapshotterSpanFactory.Builder createBuilder(SpanFactory spanFactory) {
        return DefaultSnapshotterSpanFactory.builder().spanFactory(spanFactory);
    }

    @Override
    protected DefaultSnapshotterSpanFactory createFactoryBasedOnBuilder(DefaultSnapshotterSpanFactory.Builder builder) {
        return builder.build();
    }
}