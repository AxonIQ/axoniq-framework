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

package io.axoniq.framework.extension.dataprotection.sample.giftcard.domain

import io.axoniq.framework.extension.dataprotection.sample.giftcard.command.{IssueGiftCardCommand, RedeemGiftCardCommand}
import io.axoniq.framework.extension.dataprotection.sample.giftcard.event.{GiftCardIssuedEvent, GiftCardRedeemedEvent}
import org.axonframework.eventsourcing.annotation.EventSourcingHandler
import org.axonframework.eventsourcing.annotation.reflection.EntityCreator
import org.axonframework.extension.spring.stereotype.EventSourced
import org.axonframework.messaging.commandhandling.annotation.CommandHandler
import org.axonframework.messaging.eventhandling.gateway.EventAppender

import java.math.BigDecimal

/**
 * Gift Card EventSourced DCB model implementing CQRS/Event Sourcing pattern using Axon Framework 5.
 *
 * This EventSourced DCB (Decider-based Component) model represents a gift card in the system that can be issued
 * with an initial amount and redeemed in partial amounts until the balance is exhausted. The DCB model follows
 * the event sourcing pattern where state changes are represented as immutable events.
 *
 * Business Rules:
 * - Gift cards must be issued with a positive amount
 * - Redemptions must be for positive amounts
 * - Redemptions cannot exceed the remaining balance
 * - Gift cards maintain their remaining value after redemptions
 *
 */
@EventSourced(tagKey = "giftCardId")
class GiftCard @EntityCreator() () {

  /**
   * The remaining balance on this gift card.
   * This value decreases with each redemption and is never negative.
   */
  private var remainingValue: BigDecimal = BigDecimal.ZERO

  /**
   * Command handler constructor for issuing a new gift card.
   *
   * This constructor serves as a command handler for IssueGiftCardCommand and creates
   * a new gift card DCB model instance. It validates the business rules and applies the
   * GiftCardIssuedEvent if validation succeeds.
   *
   * Business validation:
   * - Ensures the initial amount is positive (greater than zero)
   *
   * @param command the command containing gift card ID and initial amount
   * @param appender event appender for applying events
   * @throws IllegalArgumentException if the amount is not positive
   */
  @CommandHandler
  def handle(command: IssueGiftCardCommand, appender: EventAppender): Unit = {
    if (command.amount.compareTo(BigDecimal.ZERO) <= 0) {
      throw new IllegalArgumentException("Gift card amount must be positive")
    }
    appender.append(
      GiftCardIssuedEvent(
        command.giftCardId,
        command.amount,
        command.username,
        command.owner,
        command.dateOfBirth,
        command.randomNumber
      )
    )
  }

  /**
   * Command handler for redeeming an amount from an existing gift card.
   *
   * This method handles RedeemGiftCardCommand and validates business rules before
   * applying the GiftCardRedeemedEvent. The redemption reduces the remaining balance
   * of the gift card.
   *
   * Business validation:
   * - Ensures the redemption amount is positive
   * - Ensures sufficient funds are available (amount ≤ remaining balance)
   *
   * @param command the command containing gift card ID and redemption amount
   * @param appender event appender for applying events
   * @throws IllegalArgumentException if the amount is not positive or exceeds remaining balance
   */
  @CommandHandler
  def handle(command: RedeemGiftCardCommand, appender: EventAppender): Unit = {
    if (command.amount.compareTo(BigDecimal.ZERO) <= 0) {
      throw new IllegalArgumentException("Redeem amount must be positive")
    }
    if (command.amount.compareTo(remainingValue) > 0) {
      throw new IllegalArgumentException("Insufficient funds")
    }
    appender.append(GiftCardRedeemedEvent(command.giftCardId, command.amount))
  }

  /**
   * Event sourcing handler for gift card issued events.
   *
   * This method is called when a GiftCardIssuedEvent is applied to reconstruct
   * the DCB model state. It sets the initial state of the gift card with the provided
   * ID and initial balance.
   *
   * @param event the event containing gift card ID and initial amount
   */
  @EventSourcingHandler
  def on(event: GiftCardIssuedEvent): Unit = {
    this.remainingValue = event.amount
  }

  /**
   * Event sourcing handler for gift card redeemed events.
   *
   * This method is called when a GiftCardRedeemedEvent is applied to reconstruct
   * the DCB model state. It reduces the remaining balance by the redeemed amount.
   *
   * @param event the event containing gift card ID and redeemed amount
   */
  @EventSourcingHandler
  def on(event: GiftCardRedeemedEvent): Unit = {
    this.remainingValue = this.remainingValue.subtract(event.amount)
  }
}
