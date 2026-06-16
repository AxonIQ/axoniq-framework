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

package io.axoniq.framework.statecontroller.business.coverage;

import io.axoniq.framework.statecontroller.decisions.Decide;
import io.axoniq.framework.statecontroller.decisions.Decision;
import io.axoniq.framework.statecontroller.decisions.DecisionContext;
import io.axoniq.framework.statecontroller.decisions.StateController;
import io.axoniq.framework.statecontroller.decisions.UncoveredEventException;
import io.axoniq.framework.statecontroller.eventstream.EventStream;
import io.axoniq.framework.statecontroller.history.History;
import org.axonframework.eventsourcing.annotation.EventTag;
import org.axonframework.messaging.commandhandling.configuration.CommandHandlingModule;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.test.fixture.AxonTestFixture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end coverage of the Dynamic Consistency Boundary (DCB) tagging-drift guard, driven through Axon
 * Framework's {@link AxonTestFixture} exactly as the banking sample's {@code AccountsTest} is. Prior events are
 * seeded through the configured {@code EventSink} (auto-tagged off {@link EventTag @EventTag}), a command is
 * dispatched, and the appended events / thrown exception are asserted.
 * <p>
 * The fixture wires the default event-sourcing configuration: a single {@code AnnotationBasedTagResolver} both
 * registered as the {@code TagResolver} component the guard resolves and used by the event store to tag appended
 * events. The tags the guard computes therefore equal the tags the append commits, so a decision that reads a
 * tag scope but emits an event tagged differently (or not at all) is caught before anything is written.
 * <p>
 * Each {@link Nested} group pins one branch of the guard end to end:
 * <ul>
 *     <li>{@link Fires} — a {@code @Decide} decision reads {@code history.of("account", id)} then accepts an event
 *         whose tagging does not cover that scope; the command fails with an {@link UncoveredEventException} and
 *         nothing is appended;</li>
 *     <li>{@link Passes} — the same shape with the event correctly {@code @EventTag}-ged to the account commits
 *         normally;</li>
 *     <li>{@link Unconditional} — a decision that reads no {@code History} scope accepts an untagged event,
 *         proving the guard stays disabled for a legitimate creation append;</li>
 *     <li>{@link Legacy} — a {@code @StateController} decision (the {@code DecisionContext} surface) commits its
 *         events regardless of tagging, proving the guard never fires off the legacy path.</li>
 * </ul>
 *
 * @author Stefan Dragisic
 * @since 5.2.0
 */
class DcbCoverageGuardTest {

    private AxonTestFixture fixture;

    @BeforeEach
    void setUp() {
        var configurer = MessagingConfigurer.create().registerCommandHandlingModule(
                CommandHandlingModule.named("Coverage")
                                     .commandHandlers(ch ->
                                                              ch.autodetectedCommandHandlingComponent(
                                                                      c -> new CoverageDecisions()
                                                              )));
        fixture = AxonTestFixture.with(configurer);
    }

    @Nested
    class Fires {

        @Test
        void aDecideAcceptingAnEventTaggedToADifferentScopeThanWasReadIsRejectedWithoutAppending() {
            // given an existing account read via history.of("account", "a1"), but Mark emits a ledger-tagged event
            fixture.given()
                   .events(new AccountOpened("a1"))
                   // when marking the account, whose accepted event is tagged on the foreign "ledger" key
                   .when()
                   .command(new MarkAccount("a1"))
                   // then the guard rejects the drift, naming the offending event and advising @EventTag
                   .then()
                   .exceptionSatisfies(t -> {
                       UncoveredEventException ex = uncoveredEventException(t);
                       assertThat(ex.getMessage()).contains(LedgerNoted.class.getName());
                       assertThat(ex.getMessage()).contains("@EventTag");
                   })
                   // and nothing was appended for the uncovered command
                   .noEvents();
        }

        @Test
        void aDecideAcceptingAnUntaggedEventAfterAScopedReadIsRejectedWithoutAppending() {
            // given an existing account read via history.of("account", "a1")
            fixture.given()
                   .events(new AccountOpened("a1"))
                   // when touching the account, whose accepted event carries no tags at all
                   .when()
                   .command(new TouchAccount("a1"))
                   // then the untagged event is covered by no tag-scoped boundary, so the guard fires
                   .then()
                   .exceptionSatisfies(t ->
                                               assertThat(uncoveredEventException(t).getMessage())
                                                       .contains(UntaggedNote.class.getName()))
                   .noEvents();
        }
    }

    @Nested
    class Passes {

        @Test
        void aDecideAcceptingAnEventTaggedToMatchTheReadScopeCommitsNormally() {
            // given an existing account read via history.of("account", "a1")
            fixture.given()
                   .events(new AccountOpened("a1"))
                   // when noting the account with an event correctly @EventTag-ged account=a1
                   .when()
                   .command(new NoteAccount("a1"))
                   // then the covered event passes the guard and is appended
                   .then()
                   .events(new AccountNoted("a1"));
        }
    }

    @Nested
    class Unconditional {

        @Test
        void aDecideThatReadsNoHistoryScopeIsNotGuardedAndAppendsItsUntaggedEvent() {
            // given an empty store; the open decision never reads a History scope
            fixture.given()
                   .noPriorActivity()
                   // when opening an account; the accepted event carries no tags
                   .when()
                   .command(new OpenAccount("a1"))
                   // then with no recorded read boundaries the guard stays disabled and the append proceeds
                   .then()
                   .events(new AccountOpened("a1"));
        }
    }

    @Nested
    class Legacy {

        @Test
        void aStateControllerDecisionAppendsItsEventsRegardlessOfTagging() {
            // given a fixture wiring the legacy @StateController component instead of the @Decide one
            var configurer = MessagingConfigurer.create().registerCommandHandlingModule(
                    CommandHandlingModule.named("LegacyCoverage")
                                         .commandHandlers(ch ->
                                                                  ch.autodetectedCommandHandlingComponent(
                                                                          c -> new LegacyDecisions()
                                                                  )));
            var legacyFixture = AxonTestFixture.with(configurer);

            // when a legacy decision reads ctx.scope("account", "a1") then emits a completely untagged event;
            //      the legacy path records no read boundaries, so the guard must never fire
            legacyFixture.given()
                         .events(new AccountOpened("a1"))
                         .when()
                         .command(new RecordLegacyNote("a1"))
                         // then the untagged event commits, proving the legacy surface is left untouched
                         .then()
                         .events(new UntaggedNote("a1"));
        }
    }

    /**
     * Unwraps {@code thrown} to the {@link UncoveredEventException} the guard raised, walking the cause chain so
     * the assertion holds whether the command dispatch surfaces the guard exception directly or wrapped.
     */
    private static UncoveredEventException uncoveredEventException(Throwable thrown) {
        for (Throwable current = thrown; current != null; current = current.getCause()) {
            if (current instanceof UncoveredEventException ex) {
                return ex;
            }
        }
        throw new AssertionError("Expected an UncoveredEventException in the cause chain but found: " + thrown,
                                 thrown);
    }

    // ----------------------------------------------------------------------
    // Commands
    // ----------------------------------------------------------------------

    record OpenAccount(String accountId) {

    }

    record NoteAccount(String accountId) {

    }

    record TouchAccount(String accountId) {

    }

    record MarkAccount(String accountId) {

    }

    record RecordLegacyNote(String accountId) {

    }

    // ----------------------------------------------------------------------
    // Events — tagging chosen on purpose to exercise each guard branch
    // ----------------------------------------------------------------------

    /** Tagged {@code account} so it lands in {@code history.of("account", id)}; used to seed and to cover. */
    record AccountOpened(@EventTag(key = "account") String accountId) {

    }

    /** Tagged {@code account=id}: correctly covers a decision that read {@code history.of("account", id)}. */
    record AccountNoted(@EventTag(key = "account") String accountId) {

    }

    /** Carries no {@code @EventTag} at all: covered by no tag-scoped read boundary. */
    record UntaggedNote(String accountId) {

    }

    /** Tagged on the foreign {@code ledger} key: drifts away from any {@code account} scope a decision read. */
    record LedgerNoted(@EventTag(key = "ledger") String ledgerId) {

    }

    // ----------------------------------------------------------------------
    // @Decide decisions over the History surface (guarded)
    // ----------------------------------------------------------------------

    /**
     * Business-first {@code @Decide} decisions exercising the DCB coverage guard. Every command but
     * {@link OpenAccount} reads a single {@code account} scope, recording a read boundary; what each one accepts
     * decides whether the guard fires.
     */
    public static class CoverageDecisions {

        @Decide
        Decision open(OpenAccount cmd, History history) {
            // No History scope is read, so no read boundary is recorded: an unconditional creation append.
            return Decision.accept(new AccountOpened(cmd.accountId()));
        }

        @Decide
        Decision note(NoteAccount cmd, History history) {
            history.of("account", cmd.accountId()).has(AccountOpened.class);
            // Correctly tagged account=id, covering the read scope.
            return Decision.accept(new AccountNoted(cmd.accountId()));
        }

        @Decide
        Decision touch(TouchAccount cmd, History history) {
            history.of("account", cmd.accountId()).has(AccountOpened.class);
            // Untagged: covered by no tag-scoped boundary, so the guard fires.
            return Decision.accept(new UntaggedNote(cmd.accountId()));
        }

        @Decide
        Decision mark(MarkAccount cmd, History history) {
            history.of("account", cmd.accountId()).has(AccountOpened.class);
            // Tagged on a foreign ledger key: drifts away from the account scope that was read.
            return Decision.accept(new LedgerNoted(cmd.accountId()));
        }
    }

    /**
     * Legacy {@code @StateController} decision over the {@code DecisionContext} surface. It reads a scope and emits
     * an untagged event; because the legacy path records no {@code History} read boundaries the coverage guard
     * never fires here, so the event commits regardless of its tagging.
     */
    public static class LegacyDecisions {

        @StateController
        public Decision record(RecordLegacyNote cmd, DecisionContext ctx) {
            EventStream account = ctx.scope("account", cmd.accountId());
            // Force the read so the legacy scope is genuinely sourced; it still records no ReadBoundaries.
            account.contains(AccountOpened.class).isTrue();
            return Decision.emit(new UntaggedNote(cmd.accountId()));
        }
    }
}
