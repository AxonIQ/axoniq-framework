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

package io.axoniq.framework.messaging.multitenancy.eventsourcing;

import io.axoniq.framework.messaging.multitenancy.api.MetadataBasedTenantResolver;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantNotResolvedException;
import io.axoniq.framework.messaging.multitenancy.api.TenantResolver;
import io.axoniq.framework.messaging.multitenancy.api.TenantRouter;
import io.axoniq.framework.messaging.multitenancy.eventstreaming.MultiTenantTrackingToken;
import io.axoniq.framework.messaging.multitenancy.util.RecordingEventStorageEngine;
import io.axoniq.framework.messaging.multitenancy.util.RecordingSnapshotResolvingEventStorageEngine;
import io.axoniq.framework.messaging.multitenancy.util.RecordingSnapshotStore;
import io.axoniq.framework.messaging.multitenancy.util.TenantDescriptorMapping;
import org.axonframework.common.Registration;
import org.axonframework.common.infra.MockComponentDescriptor;
import org.axonframework.eventsourcing.eventstore.AppendCondition;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.GenericTaggedEventMessage;
import org.axonframework.eventsourcing.eventstore.GlobalIndexPosition;
import org.axonframework.eventsourcing.eventstore.SnapshotEventMessage;
import org.axonframework.eventsourcing.eventstore.SourcingCondition;
import org.axonframework.eventsourcing.eventstore.SourcingStrategy;
import org.axonframework.eventsourcing.eventstore.TaggedEventMessage;
import org.axonframework.eventsourcing.eventstore.inmemory.InMemoryEventStorageEngine;
import org.axonframework.eventsourcing.snapshot.api.Snapshot;
import org.axonframework.eventsourcing.snapshot.inmemory.InMemorySnapshotStore;
import org.axonframework.messaging.core.DelegatingMessageStream;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.token.GlobalSequenceTrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventstreaming.EventCriteria;
import org.axonframework.messaging.eventstreaming.StreamingCondition;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static io.axoniq.framework.messaging.multitenancy.util.TestFixtures.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

class MultiTenantEventStorageEngineTest {

    private static final SourcingCondition ANY = SourcingCondition.conditionFor(EventCriteria.havingAnyTag());
    private static final AppendCondition NONE = AppendCondition.none();
    private static final QualifiedName SNAPSHOT_NAME = new QualifiedName(String.class);
    private static final String IDENTIFIER = "identifier-1";

    private final TenantDescriptorMapping<RecordingSnapshotResolvingEventStorageEngine> engines =
            new TenantDescriptorMapping<>();
    private final RecordingSnapshotResolvingEventStorageEngine tenantA =
            engines.entry(TENANT_A, new RecordingSnapshotResolvingEventStorageEngine());
    private final RecordingSnapshotResolvingEventStorageEngine tenantB =
            engines.entry(TENANT_B, new RecordingSnapshotResolvingEventStorageEngine());
    // Each tenant's engine is its own snapshot store, so it stays undecorated and receives the strategy itself.
    private final TenantSnapshotStoreFactory snapshotStores = engines::apply;

    private MultiTenantEventStorageEngine engineWith(TenantResolver tenantResolver) {
        return registered(new MultiTenantEventStorageEngine(engines::apply,
                                                           snapshotStores,
                                                           new TenantRouter(tenantResolver, engines)));
    }

    private MultiTenantEventStorageEngine engineWith(TenantEventStorageEngineFactory engineFactory,
                                                     TenantSnapshotStoreFactory snapshotStoreFactory) {
        return registered(new MultiTenantEventStorageEngine(engineFactory,
                                                           snapshotStoreFactory,
                                                           new TenantRouter(alwaysTenant(TENANT_A), engines)));
    }

    private static MultiTenantEventStorageEngine registered(MultiTenantEventStorageEngine engine) {
        engine.registerTenant(TENANT_A);
        engine.registerTenant(TENANT_B);
        return engine;
    }

    private static EventMessage event(@Nullable TenantDescriptor tenant) {
        Map<String, String> metadata = tenant == null
                ? Map.of()
                : Map.of(TenantDescriptor.TENANT_ID_KEY, tenant.tenantId());
        return new GenericEventMessage(new MessageType("TestEvent"), "payload", metadata);
    }

    private static ProcessingContext contextFor(TenantDescriptor tenant) {
        return StubProcessingContext.forMessage(event(tenant));
    }

    private static SourcingCondition snapshotCondition() {
        return SourcingCondition.conditionFor(new SourcingStrategy.Snapshot(SNAPSHOT_NAME, IDENTIFIER, null),
                                              EventCriteria.havingAnyTag());
    }

    private static Snapshot snapshot() {
        return new Snapshot(new GlobalIndexPosition(0L), "0", "payload", Instant.EPOCH, Map.of());
    }

    private static List<TaggedEventMessage<?>> tagged(EventMessage event) {
        return List.of(new GenericTaggedEventMessage<>(event, Set.of()));
    }

    @Nested
    class Writing {

        @Test
        void appendRoutesToTheTenantOnTheProcessingContext() {
            // resolver would say TENANT_B, but the context resource wins
            MultiTenantEventStorageEngine testSubject = engineWith(alwaysTenant(TENANT_B));
            StubProcessingContext context = new StubProcessingContext();
            context.withResource(TenantDescriptor.RESOURCE_KEY, TENANT_A);

            testSubject.appendEvents(AppendCondition.none(), context, tagged(event(null)));

            assertThat(tenantA.appendCount()).isEqualTo(1);
            assertThat(tenantB.appendCount()).isZero();
        }

        @Test
        void appendWithoutContextResolvesTheTenantFromTheEventMetadata() {
            MultiTenantEventStorageEngine testSubject = engineWith(new MetadataBasedTenantResolver());

            testSubject.appendEvents(AppendCondition.none(), null, tagged(event(TENANT_B)));

            assertThat(tenantB.appendCount()).isEqualTo(1);
            assertThat(tenantA.appendCount()).isZero();
        }

        @Test
        void appendWithoutAResolvableTenantFails() {
            MultiTenantEventStorageEngine testSubject = engineWith(new MetadataBasedTenantResolver());

            var result = testSubject.appendEvents(NONE, null, tagged(event(null)));

            assertThat(result).isCompletedExceptionally();
            assertThatThrownBy(result::join).hasCauseInstanceOf(TenantNotResolvedException.class);
        }

        @Test
        void appendOfABatchSpanningTenantsFails() {
            MultiTenantEventStorageEngine testSubject = engineWith(new MetadataBasedTenantResolver());
            List<TaggedEventMessage<?>> mixed = List.of(
                    new GenericTaggedEventMessage<>(event(TENANT_A), Set.of()),
                    new GenericTaggedEventMessage<>(event(TENANT_B), Set.of())
            );

            var result = testSubject.appendEvents(NONE, null, mixed);

            assertThat(result).isCompletedExceptionally();
            assertThatThrownBy(result::join).hasCauseInstanceOf(TenantNotResolvedException.class);
        }
    }

    @Nested
    class Sourcing {

        @Test
        void sourceRoutesToTheTenantOnTheProcessingContext() {
            MultiTenantEventStorageEngine testSubject = engineWith(alwaysTenant(TENANT_B));
            StubProcessingContext context = new StubProcessingContext();
            context.withResource(TenantDescriptor.RESOURCE_KEY, TENANT_A);

            testSubject.source(ANY, context);

            assertThat(tenantA.sourceCount()).isEqualTo(1);
            assertThat(tenantB.sourceCount()).isZero();
        }

        @Test
        void sourceWithoutAContextFails() {
            MultiTenantEventStorageEngine testSubject = engineWith(alwaysTenant(TENANT_A));

            assertThat(testSubject.source(ANY, null).error())
                    .containsInstanceOf(TenantNotResolvedException.class);
        }

        @Test
        void sourceWithoutATenantResourceResolvesFromTheMessageInTheContext() {
            // no resource on the context, so the tenant is resolved from the message the context carries
            MultiTenantEventStorageEngine testSubject = engineWith(new MetadataBasedTenantResolver());
            ProcessingContext context = StubProcessingContext.forMessage(event(TENANT_B));

            testSubject.source(ANY, context);

            assertThat(tenantB.sourceCount()).isEqualTo(1);
            assertThat(tenantA.sourceCount()).isZero();
        }

        @Test
        void sourceWithAContextThatResolvesToNoTenantFails() {
            MultiTenantEventStorageEngine testSubject = engineWith(new MetadataBasedTenantResolver());
            ProcessingContext context = StubProcessingContext.forMessage(event(null));

            assertThat(testSubject.source(ANY, context).error())
                    .containsInstanceOf(TenantNotResolvedException.class);
        }

        // The tenant's engine is built to resolve that tenant's snapshots, so the routing engine forwards the sourcing
        // condition unchanged. That is what lets a snapshot resolving tenant engine keep its single round trip.
        @Test
        void snapshotSourcingReachesTheTenantEngineWithTheStrategyIntact() {
            MultiTenantEventStorageEngine testSubject = engineWith(new MetadataBasedTenantResolver());

            testSubject.source(snapshotCondition(), contextFor(TENANT_A));

            assertThat(tenantA.sourcedWithSnapshotStrategy()).isTrue();
            assertThat(tenantB.sourcedWithSnapshotStrategy()).isFalse();
        }

        // A tenant whose engine resolves no snapshots is served through the decorated route, and that decoration sits
        // below the fan-out, so the snapshot comes from that tenant's own store.
        @Test
        void snapshotSourcingOfATenantWithADecoratedEngineReadsFromThatTenantsStore() {
            RecordingEventStorageEngine plainEngine = new RecordingEventStorageEngine();
            RecordingSnapshotStore storeOfTenantA = new RecordingSnapshotStore();
            RecordingSnapshotStore storeOfTenantB = new RecordingSnapshotStore();
            MultiTenantEventStorageEngine testSubject = engineWith(
                    tenant -> plainEngine,
                    tenant -> TENANT_A.equals(tenant) ? storeOfTenantA : storeOfTenantB);

            testSubject.source(snapshotCondition(), contextFor(TENANT_A));

            assertThat(storeOfTenantA.loadCount()).isEqualTo(1);
            assertThat(storeOfTenantB.loadCount()).isZero();
            // the strategy was resolved by the decoration, so the tenant's engine was sourced by position
            assertThat(plainEngine.sourcedWithSnapshotStrategy()).isFalse();
            assertThat(plainEngine.sourceCount()).isEqualTo(1);
        }

        // Routing never resolves a snapshot itself, so a plain sourcing must not touch any tenant's snapshot store.
        @Test
        void nonSnapshotSourcingDoesNotConsultAnyTenantSnapshotStore() {
            MultiTenantEventStorageEngine testSubject = engineWith(new MetadataBasedTenantResolver());

            testSubject.source(ANY, contextFor(TENANT_A));

            assertThat(tenantA.loadCount()).isZero();
            assertThat(tenantB.loadCount()).isZero();
        }
    }

    @Nested
    class Streaming {

        private final TenantDescriptorMapping<EventStorageEngine> streamingEngines = new TenantDescriptorMapping<>();
        private final EventStorageEngine storeA = streamingEngines.entry(TENANT_A, new InMemoryEventStorageEngine());
        private final EventStorageEngine storeB = streamingEngines.entry(TENANT_B, new InMemoryEventStorageEngine());
        private final MultiTenantEventStorageEngine testSubject = streamingEngineOver(streamingEngines);

        @Test
        void streamInterleavesTheTenantsByTimestamp() {
            seed(storeA, eventAt("A1", instant(0)));
            seed(storeB, eventAt("B1", instant(1)));
            seed(storeA, eventAt("A2", instant(2)));
            seed(storeB, eventAt("B2", instant(3)));

            MessageStream<EventMessage> stream = testSubject.stream(StreamingCondition.startingFrom(null));

            assertThat(payloads(stream, 4)).containsExactly("A1", "B1", "A2", "B2");
            stream.close();
        }

        @Test
        void streamTagsEachEventWithItsTenant() {
            seed(storeA, eventAt("A1", instant(0)));
            seed(storeB, eventAt("B1", instant(1)));

            MessageStream<EventMessage> stream = testSubject.stream(StreamingCondition.startingFrom(null));

            assertThat(tenantByPayload(stream, 2))
                    .containsEntry("A1", TENANT_A)
                    .containsEntry("B1", TENANT_B);
            stream.close();
        }

        @Test
        void streamPositionsEachEventWithAMultiTenantTrackingToken() {
            seed(storeA, eventAt("A1", instant(0)));
            seed(storeB, eventAt("B1", instant(1)));

            MessageStream<EventMessage> stream = testSubject.stream(StreamingCondition.startingFrom(null));
            nextEntry(stream);
            TrackingToken token = nextEntry(stream).getResource(TrackingToken.RESOURCE_KEY);

            assertThat(token).isInstanceOf(MultiTenantTrackingToken.class);
            MultiTenantTrackingToken multiTenantToken = (MultiTenantTrackingToken) token;
            assertThat(multiTenantToken.tokenForTenant(TENANT_A.tenantId()))
                    .isEqualTo(new GlobalSequenceTrackingToken(1));
            assertThat(multiTenantToken.tokenForTenant(TENANT_B.tenantId()))
                    .isEqualTo(new GlobalSequenceTrackingToken(1));
            stream.close();
        }

        @Test
        void streamResumesFromAMultiTenantTrackingToken() {
            seed(storeA, eventAt("A1", instant(0)));
            seed(storeB, eventAt("B1", instant(1)));
            seed(storeA, eventAt("A2", instant(2)));
            seed(storeB, eventAt("B2", instant(3)));

            MessageStream<EventMessage> firstPass = testSubject.stream(StreamingCondition.startingFrom(null));
            nextEntry(firstPass);
            TrackingToken resumeToken = nextEntry(firstPass).getResource(TrackingToken.RESOURCE_KEY);
            firstPass.close();

            MessageStream<EventMessage> secondPass = testSubject.stream(StreamingCondition.startingFrom(resumeToken));

            assertThat(payloads(secondPass, 2)).containsExactly("A2", "B2");
            secondPass.close();
        }

        @Test
        void streamStartsATenantAbsentFromTheTokenAtItsBeginning() {
            seed(storeA, eventAt("A1", instant(0)));
            // a token from before tenant B existed, tracking only tenant A up to its only event
            MultiTenantTrackingToken tokenKnowingOnlyTenantA = new MultiTenantTrackingToken(
                    Map.of(TENANT_A.tenantId(), new GlobalSequenceTrackingToken(1)));
            seed(storeB, eventAt("B1", instant(1)));

            MessageStream<EventMessage> stream =
                    testSubject.stream(StreamingCondition.startingFrom(tokenKnowingOnlyTenantA));

            assertThat(tenantByPayload(stream, 1)).containsEntry("B1", TENANT_B);
            stream.close();
        }

        @Test
        void firstTokenStreamsEveryTenantFromItsBeginning() {
            seed(storeA, eventAt("A1", instant(0)));
            seed(storeB, eventAt("B1", instant(1)));

            MessageStream<EventMessage> stream =
                    testSubject.stream(StreamingCondition.startingFrom(testSubject.firstToken().join()));

            assertThat(payloads(stream, 2)).containsExactly("A1", "B1");
            stream.close();
        }

        @Test
        void firstTokenNamesNoTenantSoItSurvivesAChangeOfTenants() {
            seed(storeA, eventAt("A1", instant(0)));
            seed(storeB, eventAt("B1", instant(1)));

            // firstToken names no tenant: streaming from it opens each tenant at its own store beginning (filled in per
            // tenant), while a token written before a tenant existed stays comparable, so the processor does not rehand
            // already-handled events after a tenant change.
            assertThat(testSubject.firstToken().join()).isEqualTo(MultiTenantTrackingToken.empty());
        }

        @Test
        void latestTokenComposesThePerTenantLatestTokens() {
            seed(storeA, eventAt("A1", instant(0)));
            seed(storeB, eventAt("B1", instant(1)));
            seed(storeB, eventAt("B2", instant(2)));

            MultiTenantTrackingToken token = (MultiTenantTrackingToken) testSubject.latestToken().join();

            assertThat(token.tokenForTenant(TENANT_A.tenantId())).isEqualTo(storeA.latestToken().join());
            assertThat(token.tokenForTenant(TENANT_B.tenantId())).isEqualTo(storeB.latestToken().join());
        }

        @Test
        void tokenAtComposesThePerTenantTokens() {
            seed(storeA, eventAt("A1", instant(0)));
            seed(storeA, eventAt("A2", instant(10)));
            seed(storeB, eventAt("B1", instant(5)));

            MultiTenantTrackingToken token = (MultiTenantTrackingToken) testSubject.tokenAt(instant(5)).join();

            assertThat(token.tokenForTenant(TENANT_A.tenantId())).isEqualTo(storeA.tokenAt(instant(5)).join());
            assertThat(token.tokenForTenant(TENANT_B.tenantId())).isEqualTo(storeB.tokenAt(instant(5)).join());
        }

        @Test
        void streamOmitsATenantWhoseStoreIsEmptyWithoutError() {
            seed(storeA, eventAt("A1", instant(0)));
            seed(storeA, eventAt("A2", instant(1)));
            // storeB has no events

            MessageStream<EventMessage> stream = testSubject.stream(StreamingCondition.startingFrom(null));

            assertThat(payloads(stream, 2)).containsExactly("A1", "A2");
            stream.close();
        }

        @Test
        void firstTokenStreamsATenantThatOnlyGetsItsEventsAfterwards() {
            seed(storeA, eventAt("A1", instant(0)));
            // storeB is still empty when the token is taken
            TrackingToken firstToken = testSubject.firstToken().join();
            seed(storeB, eventAt("B1", instant(1)));

            MessageStream<EventMessage> stream = testSubject.stream(StreamingCondition.startingFrom(firstToken));

            // the tenant that was empty is not skipped: its later event still streams from its own beginning
            assertThat(payloads(stream, 2)).containsExactly("A1", "B1");
            stream.close();
        }

        @Test
        void oneTenantFailingToOpenFailsTheWholeStream() {
            TenantDescriptorMapping<EventStorageEngine> failingEngines = new TenantDescriptorMapping<>();
            InMemoryEventStorageEngine healthyStore = new InMemoryEventStorageEngine();
            failingEngines.entry(TENANT_A, healthyStore);
            failingEngines.entry(TENANT_B, new InMemoryEventStorageEngine() {
                @Override
                public MessageStream<EventMessage> stream(StreamingCondition condition) {
                    throw new IllegalStateException("cannot open the stream");
                }
            });
            MultiTenantEventStorageEngine failing = streamingEngineOver(failingEngines);
            seed(healthyStore, eventAt("A1", instant(0)));

            MessageStream<EventMessage> stream = failing.stream(StreamingCondition.startingFrom(null));

            // the read spans all tenants, so one tenant failing to open fails the whole stream: the healthy tenant's
            // event is not served
            await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> assertThat(stream.error()).isPresent());
            assertThat(stream.hasNextAvailable()).isFalse();
            stream.close();
        }

        @Test
        void aLaterTenantFailingToOpenClosesTheStreamsAlreadyOpened() {
            AtomicBoolean firstStreamClosed = new AtomicBoolean(false);
            AtomicInteger opens = new AtomicInteger();
            // Both tenants share one engine, so its stream() is invoked once per tenant regardless of which order the
            // tenants are merged in. The first invocation opens a stream that records when it is closed, the second
            // throws, so an opened stream always precedes the failing open.
            EventStorageEngine sharedEngine = new InMemoryEventStorageEngine() {
                @Override
                public MessageStream<EventMessage> stream(StreamingCondition condition) {
                    if (opens.getAndIncrement() == 0) {
                        return new DelegatingMessageStream<EventMessage, EventMessage>(MessageStream.empty()) {
                            @Override
                            public Optional<Entry<EventMessage>> next() {
                                return delegate().next();
                            }

                            @Override
                            public Optional<Entry<EventMessage>> peek() {
                                return delegate().peek();
                            }

                            @Override
                            public void close() {
                                firstStreamClosed.set(true);
                                super.close();
                            }
                        };
                    }
                    throw new IllegalStateException("cannot open the second tenant's stream");
                }
            };
            TenantDescriptorMapping<EventStorageEngine> sharedEngines = new TenantDescriptorMapping<>();
            sharedEngines.entry(TENANT_A, sharedEngine);
            sharedEngines.entry(TENANT_B, sharedEngine);
            MultiTenantEventStorageEngine failing = streamingEngineOver(sharedEngines);

            MessageStream<EventMessage> stream = failing.stream(StreamingCondition.startingFrom(null));

            await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> assertThat(stream.error()).isPresent());
            // the stream opened for the earlier tenant is closed rather than leaked when the later tenant fails to open
            assertThat(firstStreamClosed).isTrue();
            stream.close();
        }

        @Test
        void streamReturnsAFailedStreamWhenResolvingAStartPositionThrows() {
            TenantDescriptorMapping<EventStorageEngine> failingEngines = new TenantDescriptorMapping<>();
            failingEngines.entry(TENANT_A, new InMemoryEventStorageEngine());
            failingEngines.entry(TENANT_B, new InMemoryEventStorageEngine() {
                @Override
                public CompletableFuture<TrackingToken> firstToken() {
                    throw new IllegalStateException("cannot resolve the first token");
                }
            });
            MultiTenantEventStorageEngine failing = streamingEngineOver(failingEngines);

            // resolving the start position runs synchronously. A failure there must come back through the stream, so
            // stream() returns a failed stream rather than throwing
            MessageStream<EventMessage> stream = failing.stream(StreamingCondition.startingFrom(null));

            assertThat(stream.error()).isPresent();
            stream.close();
        }

        @Test
        void aTenantWhoseStreamFailsSurfacesItsErrorOnTheMergedStream() {
            TenantDescriptorMapping<EventStorageEngine> failingEngines = new TenantDescriptorMapping<>();
            InMemoryEventStorageEngine healthyStore = new InMemoryEventStorageEngine();
            failingEngines.entry(TENANT_A, healthyStore);
            failingEngines.entry(TENANT_B, new InMemoryEventStorageEngine() {
                @Override
                public MessageStream<EventMessage> stream(StreamingCondition condition) {
                    return MessageStream.failed(new IllegalStateException("cannot read the tenant"));
                }
            });
            MultiTenantEventStorageEngine failing = streamingEngineOver(failingEngines);
            seed(healthyStore, eventAt("A1", instant(0)));

            MessageStream<EventMessage> stream = failing.stream(StreamingCondition.startingFrom(null));

            // A tenant signalling failure as a failed stream, rather than throwing, surfaces its error on the merged
            // read. The pooled streaming processor aborts on that error before draining, so no tenant progresses even
            // though the merge itself still holds the healthy tenant's event.
            await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> assertThat(stream.error()).isPresent());
            stream.close();
        }

        @Test
        void streamOpensEachTenantAtItsOwnFirstTokenNotAtZero() {
            // A tenant whose store begins past zero, standing in for one whose early events were pruned: its engine's
            // firstToken() is non-zero. Streaming from the (empty) multi-tenant firstToken must open it at that real
            // position, resolved from the tenant's own engine, rather than assuming zero.
            TrackingToken prunedFirst = new GlobalSequenceTrackingToken(7);
            AtomicReference<TrackingToken> openedAt = new AtomicReference<>();
            RecordingEventStorageEngine prunedTenant = new RecordingEventStorageEngine() {
                @Override
                public CompletableFuture<TrackingToken> firstToken() {
                    return CompletableFuture.completedFuture(prunedFirst);
                }

                @Override
                public MessageStream<EventMessage> stream(StreamingCondition condition) {
                    openedAt.set(condition.position());
                    return super.stream(condition);
                }
            };
            TenantDescriptorMapping<EventStorageEngine> prunedEngines = new TenantDescriptorMapping<>();
            prunedEngines.entry(TENANT_A, prunedTenant);
            MultiTenantEventStorageEngine testSubject = streamingEngineOver(prunedEngines);

            testSubject.stream(StreamingCondition.startingFrom(testSubject.firstToken().join())).close();

            assertThat(openedAt.get()).isEqualTo(prunedFirst);
        }

        @Test
        void latestTokenReturnsAFailedFutureWhenATenantEngineThrows() {
            TenantDescriptorMapping<EventStorageEngine> failingEngines = new TenantDescriptorMapping<>();
            failingEngines.entry(TENANT_A, new InMemoryEventStorageEngine());
            failingEngines.entry(TENANT_B, new InMemoryEventStorageEngine() {
                @Override
                public CompletableFuture<TrackingToken> latestToken() {
                    throw new IllegalStateException("cannot resolve the latest token");
                }
            });
            MultiTenantEventStorageEngine failing = streamingEngineOver(failingEngines);

            // composing the per-tenant tokens touches each engine synchronously. A failure there comes back as a failed
            // future rather than throwing, matching appendEvents()
            assertThat(failing.latestToken()).isCompletedExceptionally();
        }
    }

    @Nested
    class StreamingWithoutTenants {

        private final MultiTenantEventStorageEngine testSubject = streamingEngineOver(new TenantDescriptorMapping<>());

        @Test
        void streamIsEmptyWhenNoTenantsExist() {
            MessageStream<EventMessage> stream = testSubject.stream(StreamingCondition.startingFrom(null));

            assertThat(stream.hasNextAvailable()).isFalse();
            assertThat(stream.error()).isEmpty();
            stream.close();
        }

        @Test
        void firstTokenIsEmptyWhenNoTenantsExist() {
            assertThat(testSubject.firstToken().join()).isEqualTo(MultiTenantTrackingToken.empty());
        }
    }

    /**
     * Verifies that a tenant's engine is composed to resolve that tenant's snapshots, along both routes: an engine
     * resolving snapshots itself keeps its single round trip, and any other engine is complemented with that tenant's
     * snapshot store.
     */
    @Nested
    class AnEngineResolvingSnapshotsItself {

        private final RecordingSnapshotResolvingEventStorageEngine tenantEngine =
                new RecordingSnapshotResolvingEventStorageEngine();

        // Its snapshot store for this tenant is the engine itself, as a PostgresqlEventStorageEngine tenant factory
        // would return.
        private MultiTenantEventStorageEngine testSubject() {
            return engineWith(tenant -> tenantEngine, tenant -> tenantEngine);
        }

        @Test
        void isComposedIntoTheEngineItself() {
            // decorating it would resolve the snapshot separately, costing it that single round trip
            assertThat(testSubject().engineFor(TENANT_A)).isSameAs(tenantEngine);
        }

        @Test
        void receivesTheSnapshotSourcingStrategyItself() {
            testSubject().engineFor(TENANT_A).source(snapshotCondition(), null);

            assertThat(tenantEngine.sourcedWithSnapshotStrategy()).isTrue();
            // read from its own storage, within that one call
            assertThat(tenantEngine.loadCount()).isEqualTo(1);
        }

        // The single round trip this path exists for: the engine serves the snapshot and the events following it from
        // its own storage, within the one source call.
        @Test
        void leadsItsStreamWithItsOwnSnapshotInASingleCall() {
            Snapshot snapshot = snapshot();
            tenantEngine.store(SNAPSHOT_NAME, IDENTIFIER, snapshot, null).join();

            MessageStream<EventMessage> sourced = testSubject().engineFor(TENANT_A)
                                                               .source(snapshotCondition(), null);

            EventMessage leading = sourced.next().orElseThrow().message();
            assertThat(leading).isInstanceOf(SnapshotEventMessage.class);
            assertThat(((SnapshotEventMessage) leading).payload()).isEqualTo(snapshot);
            assertThat(tenantEngine.sourceCount()).isEqualTo(1);
        }
    }

    @Nested
    class AnEngineResolvingNoSnapshots {

        private final RecordingEventStorageEngine tenantEngine = new RecordingEventStorageEngine();
        private final RecordingSnapshotStore tenantSnapshotStore = new RecordingSnapshotStore();

        private MultiTenantEventStorageEngine testSubject() {
            return engineWith(tenant -> tenantEngine, tenant -> tenantSnapshotStore);
        }

        @Test
        void readsTheSnapshotFromTheTenantsStore() {
            testSubject().engineFor(TENANT_A).source(snapshotCondition(), null);

            assertThat(tenantSnapshotStore.loadCount()).isEqualTo(1);
            // the strategy is resolved above the engine, which is then sourced by position
            assertThat(tenantEngine.sourcedWithSnapshotStrategy()).isFalse();
        }

        @Test
        void leadsItsStreamWithTheSnapshotFromTheTenantsStore() {
            Snapshot snapshot = snapshot();
            tenantSnapshotStore.store(SNAPSHOT_NAME, IDENTIFIER, snapshot, null).join();

            MessageStream<EventMessage> sourced = testSubject().engineFor(TENANT_A)
                                                               .source(snapshotCondition(), null);

            EventMessage leading = sourced.next().orElseThrow().message();
            assertThat(leading).isInstanceOf(SnapshotEventMessage.class);
            assertThat(((SnapshotEventMessage) leading).payload()).isEqualTo(snapshot);
        }

        @Test
        void passesAPlainSourcingThroughWithoutConsultingTheSnapshotStore() {
            testSubject().engineFor(TENANT_A).source(ANY, null);

            assertThat(tenantEngine.sourceCount()).isEqualTo(1);
            assertThat(tenantSnapshotStore.loadCount()).isZero();
        }
    }

    @Nested
    class Composition {

        private final RecordingSnapshotStore tenantSnapshotStore = new RecordingSnapshotStore();

        // The decision is taken per tenant, so a tenant served by a snapshot resolving engine and one served by a plain
        // engine must each get their own treatment from the same routing engine.
        @Test
        void decidesPerTenantWhenTenantsDifferInHowTheyResolveSnapshots() {
            RecordingSnapshotResolvingEventStorageEngine selfResolving =
                    new RecordingSnapshotResolvingEventStorageEngine();
            RecordingEventStorageEngine plain = new RecordingEventStorageEngine();
            MultiTenantEventStorageEngine testSubject = engineWith(
                    tenant -> TENANT_A.equals(tenant) ? selfResolving : plain,
                    tenant -> TENANT_A.equals(tenant) ? selfResolving : tenantSnapshotStore);

            testSubject.engineFor(TENANT_A).source(snapshotCondition(), null);
            testSubject.engineFor(TENANT_B).source(snapshotCondition(), null);

            // tenant A served the strategy itself, tenant B had it resolved from that tenant's own store
            assertThat(selfResolving.sourcedWithSnapshotStrategy()).isTrue();
            assertThat(plain.sourcedWithSnapshotStrategy()).isFalse();
            assertThat(tenantSnapshotStore.loadCount()).isEqualTo(1);
        }

        // A tenant's engine and snapshot store are gone once the tenant is removed, so a composed engine holding them
        // must not survive it, and composing is not repeated per operation while the tenant is there.
        @Test
        void composesOncePerTenantAndAgainAfterTheTenantIsReAdded() {
            RecordingEventStorageEngine tenantEngine = new RecordingEventStorageEngine();
            MultiTenantEventStorageEngine testSubject =
                    new MultiTenantEventStorageEngine(tenant -> tenantEngine,
                                                      tenant -> tenantSnapshotStore,
                                                      new TenantRouter(alwaysTenant(TENANT_A), engines));
            Registration registration = testSubject.registerTenant(TENANT_A);

            EventStorageEngine composed = testSubject.engineFor(TENANT_A);
            assertThat(testSubject.engineFor(TENANT_A)).isSameAs(composed);

            assertThat(registration.cancel()).isTrue();

            assertThatThrownBy(() -> testSubject.engineFor(TENANT_A))
                    .isInstanceOf(TenantNotResolvedException.class);

            // A re-added tenant never continues with the engine composed under its previous registration. That the
            // previous engine is also let go of is not observable here, since this engine only drops its reference to
            // it. The eviction itself is asserted where the callback exists, in TenantScopedCacheTest.
            testSubject.registerTenant(TENANT_A);
            assertThat(testSubject.engineFor(TENANT_A)).isNotSameAs(composed);
        }
    }


    @Nested
    class Construction {

        private final TenantRouter tenantRouter = new TenantRouter(alwaysTenant(TENANT_A), engines);

        @Test
        void rejectsANullEngineFactory() {
            assertThatThrownBy(() -> new MultiTenantEventStorageEngine(null, snapshotStores, tenantRouter))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessage("The tenant event storage engine factory must not be null");
        }

        @Test
        void rejectsANullSnapshotStoreFactory() {
            TenantEventStorageEngineFactory engineFactory = tenant -> new RecordingEventStorageEngine();

            assertThatThrownBy(() -> new MultiTenantEventStorageEngine(engineFactory, null, tenantRouter))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessage("The tenant snapshot store factory must not be null");
        }
    }

    @Test
    void describesItsFactoriesAndItsTenantRouter() {
        MultiTenantEventStorageEngine testSubject = engineWith(alwaysTenant(TENANT_A));
        MockComponentDescriptor descriptor = new MockComponentDescriptor();

        testSubject.describeTo(descriptor);

        assertThat(descriptor.getDescribedProperties())
                .containsKeys("engineFactory", "snapshotStoreFactory", "tenantRouter");
    }

    private static MultiTenantEventStorageEngine streamingEngineOver(
            TenantDescriptorMapping<EventStorageEngine> engines) {
        MultiTenantEventStorageEngine engine = new MultiTenantEventStorageEngine(
                engines::apply, tenant -> new InMemorySnapshotStore(), new TenantRouter(alwaysTenant(TENANT_A), engines));
        engines.tenants().forEach(engine::registerTenant);
        return engine;
    }

    private static void seed(EventStorageEngine engine, EventMessage event) {
        engine.appendEvents(NONE, null, tagged(event)).join().commit().join();
    }

    private static EventMessage eventAt(String payload, Instant timestamp) {
        return new GenericEventMessage("id-" + payload, new MessageType("TestEvent"), payload, Map.of(), timestamp);
    }

    private static Instant instant(int secondsFromEpoch) {
        return Instant.EPOCH.plusSeconds(secondsFromEpoch);
    }

    private static MessageStream.Entry<EventMessage> nextEntry(MessageStream<EventMessage> stream) {
        await().atMost(Duration.ofSeconds(2)).until(stream::hasNextAvailable);
        return stream.next().orElseThrow();
    }

    private static List<Object> payloads(MessageStream<EventMessage> stream, int count) {
        List<Object> payloads = new ArrayList<>();
        for (int index = 0; index < count; index++) {
            payloads.add(nextEntry(stream).message().payload());
        }
        return payloads;
    }

    private static Map<Object, TenantDescriptor> tenantByPayload(MessageStream<EventMessage> stream, int count) {
        Map<Object, TenantDescriptor> tenantByPayload = new HashMap<>();
        for (int index = 0; index < count; index++) {
            MessageStream.Entry<EventMessage> entry = nextEntry(stream);
            tenantByPayload.put(entry.message().payload(), entry.getResource(TenantDescriptor.RESOURCE_KEY));
        }
        return tenantByPayload;
    }
}
