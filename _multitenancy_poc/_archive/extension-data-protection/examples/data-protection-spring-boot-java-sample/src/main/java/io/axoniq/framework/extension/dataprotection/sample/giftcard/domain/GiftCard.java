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

package io.axoniq.framework.extension.dataprotection.sample.giftcard.domain;

import io.axoniq.framework.extension.dataprotection.sample.giftcard.command.IssueGiftCardCommand;
import io.axoniq.framework.extension.dataprotection.sample.giftcard.command.RedeemGiftCardCommand;
import io.axoniq.framework.extension.dataprotection.sample.giftcard.event.GiftCardIssuedEvent;
import io.axoniq.framework.extension.dataprotection.sample.giftcard.event.GiftCardRedeemedEvent;
import org.axonframework.eventsourcing.annotation.EventSourcingHandler;
import org.axonframework.eventsourcing.annotation.reflection.EntityCreator;
import org.axonframework.extension.spring.stereotype.EventSourced;
import org.axonframework.messaging.commandhandling.annotation.CommandHandler;
import org.axonframework.messaging.eventhandling.gateway.EventAppender;

import java.math.BigDecimal;

/**
 * Gift Card EventSourced DCB model implementing CQRS/Event Sourcing pattern using Axon Framework 5.
 *
 * <p>This EventSourced DCB (Decider-based Component) model represents a gift card in the system that can be issued with an initial amount
 * and redeemed in partial amounts until the balance is exhausted. The DCB model follows the
 * event sourcing pattern where state changes are represented as immutable events.</p>
 *
 * <p>Business Rules:</p>
 * <ul>
 *   <li>Gift cards must be issued with a positive amount</li>
 *   <li>Redemptions must be for positive amounts</li>
 *   <li>Redemptions cannot exceed the remaining balance</li>
 *   <li>Gift cards maintain their remaining value after redemptions</li>
 * </ul>
 *
 */
@EventSourced(tagKey = "giftCardId")
public class GiftCard {

    /**
     * The remaining balance on this gift card.
     * This value decreases with each redemption and is never negative.
     */
    private BigDecimal remainingValue = BigDecimal.ZERO;

    /**
     * Default constructor required by Axon Framework for DCB model reconstruction.
     * This constructor is used internally by the framework and should not be called directly.
     */
    @EntityCreator
    protected GiftCard() {
    }

    /**
     * Command handler constructor for issuing a new gift card.
     *
     * <p>This constructor serves as a command handler for {@link IssueGiftCardCommand} and creates
     * a new gift card DCB model instance. It validates the business rules and applies the
     * {@link GiftCardIssuedEvent} if validation succeeds.</p>
     *
     * <p>Business validation:</p>
     * <ul>
     *   <li>Ensures the initial amount is positive (greater than zero)</li>
     * </ul>
     *
     * @param command the command containing gift card ID and initial amount
     * @throws IllegalArgumentException if the amount is not positive
     */
    @CommandHandler
    public void handle(IssueGiftCardCommand command, EventAppender appender) {
        if (command.amount().compareTo(BigDecimal.ZERO) <= 0) {
            throw new IllegalArgumentException("Gift card amount must be positive");
        }
        appender.append(new GiftCardIssuedEvent(
                command.giftCardId(),
                command.amount(),
                command.username(),
                command.owner(),
                command.dateOfBirth(),
                command.randomNumber()
        ));
    }

    /**
     * Command handler for redeeming an amount from an existing gift card.
     *
     * <p>This method handles {@link RedeemGiftCardCommand} and validates business rules before
     * applying the {@link GiftCardRedeemedEvent}. The redemption reduces the remaining balance
     * of the gift card.</p>
     *
     * <p>Business validation:</p>
     * <ul>
     *   <li>Ensures the redemption amount is positive</li>
     *   <li>Ensures sufficient funds are available (amount ≤ remaining balance)</li>
     * </ul>
     *
     * @param command the command containing gift card ID and redemption amount
     * @throws IllegalArgumentException if the amount is not positive or exceeds remaining balance
     */
    @CommandHandler
    public void handle(RedeemGiftCardCommand command, EventAppender appender) {
        if (command.amount().compareTo(BigDecimal.ZERO) <= 0) {
            throw new IllegalArgumentException("Redeem amount must be positive");
        }
        if (command.amount().compareTo(remainingValue) > 0) {
            throw new IllegalArgumentException("Insufficient funds");
        }
        appender.append(new GiftCardRedeemedEvent(command.giftCardId(), command.amount()));
    }

    /**
     * Event sourcing handler for gift card issued events.
     *
     * <p>This method is called when a {@link GiftCardIssuedEvent} is applied to reconstruct
     * the DCB model state. It sets the initial state of the gift card with the provided
     * ID and initial balance.</p>
     *
     * @param event the event containing gift card ID and initial amount
     */
    @EventSourcingHandler
    public void on(GiftCardIssuedEvent event) {
        this.remainingValue = event.amount();
    }

    /**
     * Event sourcing handler for gift card redeemed events.
     *
     * <p>This method is called when a {@link GiftCardRedeemedEvent} is applied to reconstruct
     * the DCB model state. It reduces the remaining balance by the redeemed amount.</p>
     *
     * @param event the event containing gift card ID and redeemed amount
     */
    @EventSourcingHandler
    public void on(GiftCardRedeemedEvent event) {
        this.remainingValue = this.remainingValue.subtract(event.amount());
    }
}
