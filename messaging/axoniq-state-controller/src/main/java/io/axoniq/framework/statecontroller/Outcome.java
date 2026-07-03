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

package io.axoniq.framework.statecontroller;

import org.jspecify.annotations.Nullable;

import java.util.List;
import java.util.Objects;

/**
 * The result of a state-controlled command handler: either an {@link Accept} carrying the events to append
 * (optionally with a result value to surface to the command caller), or a {@link Reject} carrying a reason (and
 * optionally an audit trail).
 * <p>
 * Returning an {@code Outcome} from a plain
 * {@link org.axonframework.messaging.commandhandling.annotation.CommandHandler @CommandHandler} method is what
 * opts that handler into the State Controller: no dedicated annotation exists. The framework reads the outcome
 * and translates an {@link Accept} into a conditional append against the event store using the DCB consistency
 * boundary recorded during the decision's {@link History} reads, and a {@link Reject} into a denial of the
 * command (with audit events, if any, appended through the same {@code EventStoreTransaction}). Handlers may
 * equally return {@code CompletableFuture<Outcome>} for a fully non-blocking body; the translation is identical.
 * <p>
 * Outcomes are values. They never mutate state directly. Use the static factories — designed for static import so
 * a decision body reads as plain language:
 * <pre>{@code
 * import static io.axoniq.framework.statecontroller.Outcome.accept;
 * import static io.axoniq.framework.statecontroller.Outcome.reject;
 *
 * @CommandHandler
 * public Outcome withdraw(Withdraw cmd, History history) {
 *     History account = history.of("account", cmd.accountId());
 *     var closed  = account.has(AccountClosed.class);
 *     var balance = account.total(MoneyDeposited.class, MoneyDeposited::amount)
 *                          .minus(account.total(MoneyWithdrawn.class, MoneyWithdrawn::amount));
 *
 *     if (closed.resolve())                              return reject("account closed");
 *     if (balance.resolve().compareTo(cmd.amount()) < 0) return reject("insufficient funds");
 *     return accept(new MoneyWithdrawn(cmd.accountId(), cmd.amount()));
 * }
 * }</pre>
 * When the command caller needs to receive a value (for example a generated identifier), chain
 * {@link Accept#returning(Object)} on the accept; if no result is supplied, the caller observes {@code null}.
 * Chain {@link Reject#recording(Object...)} on a reject to leave an audit trail.
 *
 * @author Allard Buijze
 * @author Stefan Dragisic
 * @since 5.2.0
 */
public sealed interface Outcome {

    /**
     * Indicates the command was accepted and produces the events to append.
     *
     * @param events the events to append to the event store under the recorded DCB consistency boundary
     * @param result optional value to surface to the command caller as the handler's return value; {@code null}
     *               when the caller does not need a result
     */
    record Accept(List<Object> events, @Nullable Object result) implements Outcome {

        /**
         * Compact constructor producing a defensive immutable copy of {@code events}.
         */
        public Accept {
            Objects.requireNonNull(events, "events must not be null");
            events = List.copyOf(events);
        }

        /**
         * Returns a copy of this acceptance that carries the given {@code result} back to the command caller.
         * <p>
         * Common use: a command handler that creates a new entity may want to surface the generated identifier
         * as the command's return value, while still appending the underlying creation event(s).
         *
         * @param result the value to return from the command dispatch; may be {@code null} to explicitly clear a
         *               previously set result
         * @return an {@code Accept} carrying the same events and the given {@code result}
         */
        public Accept returning(@Nullable Object result) {
            return new Accept(events, result);
        }
    }

    /**
     * Indicates the command was rejected. Carries a human-readable reason and an optional list of audit events
     * appended through the same {@code EventStoreTransaction} so a rejection can still leave a trace.
     *
     * @param reason      a short reason for the rejection, surfaced to the caller via
     *                    {@link org.axonframework.messaging.commandhandling.CommandExecutionException#getMessage()
     *                    CommandExecutionException.getMessage()}
     * @param auditEvents events to append regardless of the rejection; surfaced to the caller via
     *                    {@link org.axonframework.messaging.commandhandling.CommandExecutionException#getDetails()
     *                    CommandExecutionException.getDetails()}
     */
    record Reject(String reason, List<Object> auditEvents) implements Outcome {

        /**
         * Compact constructor validating non-null arguments and producing a defensive immutable copy of
         * {@code auditEvents}.
         */
        public Reject {
            Objects.requireNonNull(reason, "reason must not be null");
            Objects.requireNonNull(auditEvents, "auditEvents must not be null");
            auditEvents = List.copyOf(auditEvents);
        }

        /**
         * Returns a copy of this rejection whose audit events are replaced with the given {@code events}. Any
         * previously-recorded audit events on this {@code Reject} are discarded; recording is therefore
         * idempotent rather than additive.
         *
         * @param events the events to append as audit trail alongside the rejection; the array and its
         *               elements must not be {@code null}
         * @return a {@code Reject} carrying the same reason and the given audit events
         */
        public Reject recording(Object... events) {
            Objects.requireNonNull(events, "events must not be null");
            return new Reject(reason, List.of(events));
        }
    }

    /**
     * Returns an {@link Accept} outcome carrying the given events, to be appended under the recorded DCB
     * consistency boundary. Chain {@link Accept#returning(Object)} to attach a result for the command caller.
     * <p>
     * Calling {@code accept()} with no events is a valid idempotent success: nothing is appended and the caller
     * observes a normal completion.
     *
     * @param events the events to append on success; the array and its elements must not be {@code null}
     * @return an {@code Accept} outcome over those events with no result
     */
    static Accept accept(Object... events) {
        Objects.requireNonNull(events, "events must not be null");
        return new Accept(List.of(events), null);
    }

    /**
     * Returns a {@link Reject} outcome with the given reason and no audit events. Chain
     * {@link Reject#recording(Object...)} on the result to attach an audit trail.
     *
     * @param reason a short reason for the rejection, surfaced to the caller
     * @return a {@code Reject} outcome carrying {@code reason}
     */
    static Reject reject(String reason) {
        return new Reject(reason, List.of());
    }
}
