/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.messaging.transformation.snapshots;

import io.axoniq.framework.messaging.transformation.ChainConfigurationException;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.conversion.Converter;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.eventsourcing.snapshot.api.SnapshotCandidate;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.VersionedType;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;

import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test class validating the {@link SnapshotTransformerChain}, and the {@link SnapshotTransformation} paths it is built
 * from.
 *
 * @author John Hendrikx
 */
class SnapshotTransformerChainTest {

    record AccountV1(String id, long cents) {}

    record AccountV2(String id, long balance) {}

    record LegacyAccount(String accountId, long amount) {}

    private static final VersionedType ACCOUNT_V1 = VersionedType.of("account", "1.0.0");
    private static final VersionedType ACCOUNT_V2 = VersionedType.of("account", "2.0.0");
    private static final VersionedType LEDGER_V2 = VersionedType.of("ledger", "2.0.0");
    private static final VersionedType AGGREGATE_ACCOUNT = VersionedType.of("com.example.Account", "0.0.0");
    private static final VersionedType AGGREGATE_ACCOUNT_REVISION_1 = VersionedType.of("com.example.Account", "1");

    private static final Converter CONVERTER = new JacksonConverter();

    private static SnapshotTransformation accountToLedgerPath() {
        return SnapshotTransformation.pathFrom(ACCOUNT_V1)
                                     .to("2.0.0")
                                     .transform(AccountV1.class, v1 -> new AccountV2(v1.id(), v1.cents() / 100))
                                     .renameTo(LEDGER_V2)
                                     .build();
    }

    @Nested
    class Paths {

        @Test
        void shouldRegisterEveryEdgeOfAPathWhenRegisteringItAsOneTransformation() {
            // when
            SnapshotTransformerChain chain = SnapshotTransformerChain.builder().register(accountToLedgerPath()).build();

            // then
            assertThat(chain.lookupFor(LEDGER_V2).candidates())
                    .containsExactly(candidate(ACCOUNT_V2), candidate(ACCOUNT_V1));
        }

        @Test
        void shouldRejectAStepToTheVersionAlreadyReached() {
            assertThatThrownBy(() -> SnapshotTransformation.pathFrom(ACCOUNT_V1).to("1.0.0"))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void shouldRejectARenameToTheIdentityAlreadyReached() {
            assertThatThrownBy(() -> SnapshotTransformation.pathFrom(ACCOUNT_V1).renameTo(ACCOUNT_V1))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void shouldTreatAMessageTypeAsTheIdentityItDescribes() {
            // given
            SnapshotTransformation path = SnapshotTransformation.pathFrom(new MessageType("account", "1.0.0"))
                                                                .renameTo(new MessageType("ledger", "1.0.0"))
                                                                .build();

            // when
            SnapshotTransformerChain chain = SnapshotTransformerChain.builder().register(path).build();

            // then
            assertThat(chain.lookupFor(VersionedType.of("ledger", "1.0.0")).candidates())
                    .containsExactly(candidate(ACCOUNT_V1));
        }
    }

    @Nested
    class Lookups {

        @Test
        void shouldOrderCandidatesNearestFirst() {
            // given
            SnapshotTransformerChain chain = SnapshotTransformerChain.builder().register(accountToLedgerPath()).build();

            // when
            SnapshotTransformerChain.Lookup lookup = chain.lookupFor(LEDGER_V2);

            // then
            assertThat(lookup.candidates()).containsExactly(candidate(ACCOUNT_V2), candidate(ACCOUNT_V1));
            assertThat(lookup.aggregateBasedCandidate()).isNull();
        }

        @Test
        void shouldFindNothingForATypeNoPathLeadsTo() {
            // given
            SnapshotTransformerChain chain = SnapshotTransformerChain.builder().register(accountToLedgerPath()).build();

            // when
            SnapshotTransformerChain.Lookup lookup = chain.lookupFor(VersionedType.of("customer", "1.0.0"));

            // then
            assertThat(lookup.candidates()).isEmpty();
            assertThat(lookup.aggregateBasedCandidate()).isNull();
        }

        @Test
        void shouldOfferEveryAggregateBasedRevisionLeadingToTheTypeAsOneCandidate() {
            // given
            SnapshotTransformerChain chain = SnapshotTransformerChain.builder()
                    .register(accountToLedgerPath())
                    .register(aggregateBasedPath(AGGREGATE_ACCOUNT, CONVERTER))
                    .register(SnapshotTransformation.pathFromAggregateBased(AGGREGATE_ACCOUNT_REVISION_1)
                                                    .storedAs(CONVERTER)
                                                    .to(ACCOUNT_V2)
                                                    .transform(LegacyAccount.class,
                                                               a -> new AccountV2(a.accountId(), a.amount()))
                                                    .build())
                    .build();

            // when
            SnapshotTransformerChain.Lookup lookup = chain.lookupFor(LEDGER_V2);

            // then
            assertThat(lookup.candidates()).containsExactly(candidate(ACCOUNT_V2), candidate(ACCOUNT_V1));
            assertThat(lookup.aggregateBasedCandidate()).isEqualTo(new SnapshotCandidate(
                    AGGREGATE_ACCOUNT.qualifiedName(),
                    Set.of(AGGREGATE_ACCOUNT.version(), AGGREGATE_ACCOUNT_REVISION_1.version())
            ));
        }
    }

    @Nested
    class Lifting {

        @Test
        void shouldApplyEveryEdgeFromTheFoundIdentityToTheTarget() {
            // given
            SnapshotTransformerChain chain = SnapshotTransformerChain.builder().register(accountToLedgerPath()).build();

            // when
            Object lifted = chain.lift(ACCOUNT_V1, false, json("{\"id\":\"a\",\"cents\":1250}"), LEDGER_V2,
                                       CONVERTER, null);

            // then
            assertThat(lifted).isEqualTo(new AccountV2("a", 12));
        }

        @Test
        void shouldLiftFromAnIdentityHalfwayAlongAPath() {
            // given
            SnapshotTransformerChain chain = SnapshotTransformerChain.builder().register(accountToLedgerPath()).build();
            AccountV2 state = new AccountV2("a", 12);

            // when
            Object lifted = chain.lift(ACCOUNT_V2, false, state, LEDGER_V2, CONVERTER, null);

            // then
            assertThat(lifted).isSameAs(state);
        }

        @Test
        void shouldReadAnAggregateBasedSnapshotWithTheConverterItIsStoredWith() {
            // given
            RecordingConverter storedWith = new RecordingConverter();
            SnapshotTransformerChain chain = SnapshotTransformerChain.builder()
                    .register(accountToLedgerPath())
                    .register(aggregateBasedPath(AGGREGATE_ACCOUNT, storedWith))
                    .build();

            // when
            Object lifted = chain.lift(AGGREGATE_ACCOUNT, true, json("{\"accountId\":\"a\",\"amount\":12}"), LEDGER_V2,
                                       CONVERTER, null);

            // then
            assertThat(lifted).isEqualTo(new AccountV2("a", 12));
            assertThat(storedWith.conversions).isEqualTo(1);
        }

        @Test
        void shouldNotTreatAnAggregateBasedIdentityAsARegularOne() {
            // given
            SnapshotTransformerChain chain = SnapshotTransformerChain.builder()
                    .register(accountToLedgerPath())
                    .register(aggregateBasedPath(AGGREGATE_ACCOUNT, CONVERTER))
                    .build();

            // when
            Object lifted = chain.lift(AGGREGATE_ACCOUNT, false, json("{\"accountId\":\"a\",\"amount\":12}"), LEDGER_V2,
                                       CONVERTER, null);

            // then
            assertThat(lifted).isNull();
        }

        @Test
        void shouldDeclineWhenAMapperReturnsNull() {
            // given
            SnapshotTransformation declining = SnapshotTransformation.pathFrom(ACCOUNT_V1)
                                                                     .to("2.0.0")
                                                                     .transform(AccountV1.class, v1 -> null)
                                                                     .build();
            SnapshotTransformerChain chain = SnapshotTransformerChain.builder().register(declining).build();

            // when
            Object lifted = chain.lift(ACCOUNT_V1, false, new AccountV1("a", 1), ACCOUNT_V2, CONVERTER, null);

            // then
            assertThat(lifted).isNull();
        }

        @Test
        void shouldDeclineWhenNoPathLeadsToTheTarget() {
            // given
            SnapshotTransformerChain chain = SnapshotTransformerChain.builder().register(accountToLedgerPath()).build();

            // when
            Object lifted = chain.lift(ACCOUNT_V1, false, new AccountV1("a", 1), VersionedType.of("other", "1"),
                                       CONVERTER, null);

            // then
            assertThat(lifted).isNull();
        }

        @Test
        void shouldDeclineASnapshotOfARefusedIdentity() {
            // given
            SnapshotTransformerChain chain = SnapshotTransformerChain.builder()
                    .register(accountToLedgerPath())
                    .register(SnapshotTransformation.rebuild(VersionedType.of("account", "0.9.0")))
                    .build();

            // when
            Object lifted = chain.lift(VersionedType.of("account", "0.9.0"), false, new AccountV1("a", 1), LEDGER_V2,
                                       CONVERTER, null);

            // then
            assertThat(lifted).isNull();
        }

        @Test
        void shouldPassTheProcessingContextToTheMapper() {
            // given
            AtomicReference<@Nullable ProcessingContext> seen = new AtomicReference<>();
            SnapshotTransformation path = SnapshotTransformation.pathFrom(ACCOUNT_V1)
                    .to("2.0.0")
                    .transform(AccountV1.class, (AccountV1 v1, @Nullable ProcessingContext context) -> {
                        seen.set(context);
                        return new AccountV2(v1.id(), v1.cents());
                    })
                    .build();
            SnapshotTransformerChain chain = SnapshotTransformerChain.builder().register(path).build();
            ProcessingContext context = new StubProcessingContext();

            // when
            chain.lift(ACCOUNT_V1, false, new AccountV1("a", 1), ACCOUNT_V2, CONVERTER, context);

            // then
            assertThat(seen.get()).isSameAs(context);
        }
    }

    @Nested
    class Configuration {

        @Test
        void shouldRejectTwoTransformationsLiftingFromTheSameIdentity() {
            // given
            SnapshotTransformerChain.Builder builder =
                    SnapshotTransformerChain.builder().register(accountToLedgerPath());
            SnapshotTransformation competing = SnapshotTransformation.pathFrom(ACCOUNT_V1)
                                                                     .renameTo(VersionedType.of("other", "1.0.0"))
                                                                     .build();

            // when / then
            assertThatThrownBy(() -> builder.register(competing)).isInstanceOf(ChainConfigurationException.class);
        }

        @Test
        void shouldNameARefusalByItsDescription() {
            // given
            SnapshotTransformerChain.Builder builder = SnapshotTransformerChain.builder()
                    .register(accountToLedgerPath())
                    .register(SnapshotTransformation.rebuild(
                            "every account", type -> type.qualifiedName().name().equals("account")
                    ));

            // when / then
            assertThatThrownBy(builder::build).isInstanceOf(ChainConfigurationException.class)
                                              .hasMessageContaining("rebuild every account");
        }

        @Test
        void shouldAllowRegisteringTheSamePathTwice() {
            // given
            SnapshotTransformation path = accountToLedgerPath();

            // when
            SnapshotTransformerChain chain = SnapshotTransformerChain.builder().register(path).register(path).build();

            // then
            assertThat(chain.lookupFor(LEDGER_V2).candidates()).hasSize(2);
        }

        @Test
        void shouldRejectACycle() {
            // given
            SnapshotTransformerChain.Builder builder = SnapshotTransformerChain.builder()
                    .register(SnapshotTransformation.pathFrom(ACCOUNT_V1).renameTo(LEDGER_V2).build())
                    .register(SnapshotTransformation.pathFrom(LEDGER_V2).renameTo(ACCOUNT_V1).build());

            // when / then
            assertThatThrownBy(builder::build).isInstanceOf(ChainConfigurationException.class);
        }

        @Test
        void shouldRejectRefusingAnIdentityThatIsAlsoLiftedFrom() {
            // given
            SnapshotTransformerChain.Builder builder = SnapshotTransformerChain.builder()
                    .register(accountToLedgerPath())
                    .register(SnapshotTransformation.rebuild(type -> type.qualifiedName().name().equals("account")));

            // when / then
            assertThatThrownBy(builder::build).isInstanceOf(ChainConfigurationException.class);
        }

        @Test
        void shouldRejectAggregateBasedSnapshotsOfSeveralPayloadTypesLeadingToOneType() {
            // given
            SnapshotTransformerChain.Builder builder = SnapshotTransformerChain.builder()
                    .register(accountToLedgerPath())
                    .register(aggregateBasedPath(AGGREGATE_ACCOUNT, CONVERTER))
                    .register(SnapshotTransformation.pathFromAggregateBased(
                                                            VersionedType.of("com.example.Ledger", "0.0.0"))
                                                    .storedAs(CONVERTER)
                                                    .to(ACCOUNT_V2)
                                                    .transform(LegacyAccount.class,
                                                               a -> new AccountV2(a.accountId(), a.amount()))
                                                    .build());

            // when / then
            assertThatThrownBy(builder::build).isInstanceOf(ChainConfigurationException.class);
        }
    }

    private static SnapshotTransformation aggregateBasedPath(VersionedType aggregateBasedSource, Converter storedWith) {
        return SnapshotTransformation.pathFromAggregateBased(aggregateBasedSource)
                                     .storedAs(storedWith)
                                     .to(ACCOUNT_V1)
                                     .transform(LegacyAccount.class,
                                                a -> new AccountV1(a.accountId(), a.amount() * 100))
                                     .build();
    }

    private static SnapshotCandidate candidate(VersionedType type) {
        return new SnapshotCandidate(type.qualifiedName(), Set.of(type.version()));
    }

    private static byte[] json(String json) {
        return json.getBytes(StandardCharsets.UTF_8);
    }

    /*
     * A Jackson converter counting its conversions, to tell it apart from the default converter.
     */
    private static final class RecordingConverter implements Converter {

        private final Converter delegate = new JacksonConverter();
        private int conversions;

        @Override
        public <T> @Nullable T convert(@Nullable Object input, Type targetType) {
            conversions++;
            return delegate.convert(input, targetType);
        }

        @Override
        public void describeTo(ComponentDescriptor descriptor) {
        }
    }
}
