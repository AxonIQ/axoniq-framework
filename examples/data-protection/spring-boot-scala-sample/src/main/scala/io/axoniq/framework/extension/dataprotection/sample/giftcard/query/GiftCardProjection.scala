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

package io.axoniq.framework.extension.dataprotection.sample.giftcard.query

import io.axoniq.framework.extension.dataprotection.sample.giftcard.event.{GiftCardIssuedEvent, GiftCardRedeemedEvent}
import org.axonframework.messaging.eventhandling.annotation.{EventHandler, Timestamp}
import org.axonframework.messaging.eventhandling.replay.annotation.ResetHandler
import org.axonframework.messaging.queryhandling.QueryUpdateEmitter
import org.axonframework.messaging.queryhandling.annotation.QueryHandler
import org.slf4j.{Logger, LoggerFactory}
import org.springframework.stereotype.Component

import java.time.{Instant, OffsetDateTime, ZoneOffset}
import java.util.function.Predicate
import scala.jdk.OptionConverters._

/**
 * Event-driven projection that maintains a read model of gift card data.
 *
 */
@Component
class GiftCardProjection(giftCardRepository: GiftCardRepository) {

  private val logger: Logger = LoggerFactory.getLogger(classOf[GiftCardProjection])

  /**
   * Event handler for gift card issued events.
   */
  @EventHandler
  def on(event: GiftCardIssuedEvent, queryUpdateEmitter: QueryUpdateEmitter, @Timestamp timestamp: Instant): Unit = {
    // Create and persist entity
    val entity: GiftCardEntity = new GiftCardEntity(
      event.giftCardId,
      event.amount,
      event.amount,
      event.username,
      event.owner,
      event.dateOfBirth,
      event.randomNumber,
      OffsetDateTime.ofInstant(timestamp, ZoneOffset.UTC)
    )
    giftCardRepository.save(entity)

    // Convert to summary for query updates
    val giftCard = new GiftCardSummary(
      entity.getGiftCardId,
      entity.getRemainingValue,
      entity.getInitialValue,
      event.username,
      event.owner,
      event.dateOfBirth,
      event.randomNumber
    )

    queryUpdateEmitter.emit(
      classOf[FindGiftCardQuery],
      new Predicate[FindGiftCardQuery] {
        override def test(query: FindGiftCardQuery): Boolean = query.giftCardId == event.giftCardId
      },
      giftCard
    )

    queryUpdateEmitter.emit(
      classOf[FindAllGiftCardsQuery],
      new Predicate[FindAllGiftCardsQuery] {
        override def test(query: FindAllGiftCardsQuery): Boolean = true
      },
      giftCard
    )
  }

  /**
   * Event handler for gift card redeemed events.
   */
  @EventHandler
  def on(event: GiftCardRedeemedEvent, queryUpdateEmitter: QueryUpdateEmitter, @Timestamp timestamp: Instant): Unit = {
    giftCardRepository.findByGiftCardId(event.giftCardId).toScala.foreach { entity =>
      // Update remaining value and last modified date
      entity.setRemainingValue(entity.getRemainingValue.subtract(event.amount))
      entity.setLastModifiedDate(OffsetDateTime.ofInstant(timestamp, ZoneOffset.UTC))
      giftCardRepository.save(entity)

      // Convert to summary for query updates - preserve existing encrypted data
      val updatedGiftCard = new GiftCardSummary(
        entity.getGiftCardId,
        entity.getRemainingValue,
        entity.getInitialValue,
        entity.getUsername,
        null, // owner not updated on redeem
        null, // dateOfBirth not updated on redeem
        null  // randomNumber not updated on redeem
      )

      queryUpdateEmitter.emit(
        classOf[FindGiftCardQuery],
        new Predicate[FindGiftCardQuery] {
          override def test(query: FindGiftCardQuery): Boolean = query.giftCardId == event.giftCardId
        },
        updatedGiftCard
      )

      queryUpdateEmitter.emit(
        classOf[FindAllGiftCardsQuery],
        new Predicate[FindAllGiftCardsQuery] {
          override def test(query: FindAllGiftCardsQuery): Boolean = true
        },
        updatedGiftCard
      )
    }
  }

  /**
   * Query handler for retrieving a specific gift card by ID.
   */
  @QueryHandler
  def handle(query: FindGiftCardQuery): GiftCardSummary = {
    val optional = giftCardRepository.findByGiftCardId(query.giftCardId)
    if (optional.isPresent) {
      val entity = optional.get()
      new GiftCardSummary(
        entity.getGiftCardId,
        entity.getRemainingValue,
        entity.getInitialValue,
        entity.getUsername,
        entity.getOwner,
        entity.getDateOfBirth,
        entity.getRandomNumber
      )
    } else {
      null
    }
  }

  /**
   * Query handler for retrieving all gift cards in the system.
   */
  @QueryHandler
  def handle(query: FindAllGiftCardsQuery): GiftCardSummaryList = {
    // Load all entities and convert to summaries
    import scala.jdk.CollectionConverters._
    val summaries = giftCardRepository.findAll().asScala.map { entity =>
      new GiftCardSummary(
        entity.getGiftCardId,
        entity.getRemainingValue,
        entity.getInitialValue,
        entity.getUsername,
        entity.getOwner,
        entity.getDateOfBirth,
        entity.getRandomNumber
      )
    }.toList

    new GiftCardSummaryList(summaries.asJava)
  }

  @ResetHandler
  def onReset(): Unit = {
    logger.info("Resetting account repository")
    logger.info("Removing {} records.", giftCardRepository.count())
    giftCardRepository.deleteAllInBatch()
  }
}
