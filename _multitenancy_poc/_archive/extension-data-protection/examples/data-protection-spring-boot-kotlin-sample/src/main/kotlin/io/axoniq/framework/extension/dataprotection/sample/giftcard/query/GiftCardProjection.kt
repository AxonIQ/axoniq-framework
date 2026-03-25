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

import io.axoniq.framework.extension.dataprotection.sample.giftcard.event.GiftCardIssuedEvent
import io.axoniq.framework.extension.dataprotection.sample.giftcard.event.GiftCardRedeemedEvent
import org.axonframework.messaging.eventhandling.annotation.EventHandler
import org.axonframework.messaging.eventhandling.annotation.Timestamp
import org.axonframework.messaging.eventhandling.replay.annotation.ResetHandler
import org.axonframework.messaging.queryhandling.QueryUpdateEmitter
import org.axonframework.messaging.queryhandling.annotation.QueryHandler
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.function.Predicate

/**
 * Event-driven projection that maintains a read model of gift card data.
 *
 */
@Component
class GiftCardProjection(private val giftCardRepository: GiftCardRepository) {

    private val logger = LoggerFactory.getLogger(GiftCardProjection::class.java)

    /**
     * Event handler for gift card issued events.
     */
    @EventHandler
    fun on(event: GiftCardIssuedEvent, queryUpdateEmitter: QueryUpdateEmitter, @Timestamp timestamp: Instant) {
        // Create and persist entity
        val entity = GiftCardEntity(
            giftCardId = event.giftCardId,
            initialValue = event.amount,
            remainingValue = event.amount,
            username = event.username,
            owner = event.owner,
            dateOfBirth = event.dateOfBirth,
            randomNumber = event.randomNumber,
            creationDate = OffsetDateTime.ofInstant(timestamp, ZoneOffset.UTC)
        )
        giftCardRepository.save(entity)

        // Convert to summary for query updates
        val giftCard = GiftCardSummary(
            giftCardId = entity.giftCardId!!,
            remainingValue = entity.remainingValue!!,
            initialValue = entity.initialValue!!,
            username = event.username,
            owner = event.owner,
            dateOfBirth = event.dateOfBirth,
            randomNumber = event.randomNumber
        )

        queryUpdateEmitter.emit(
            FindGiftCardQuery::class.java,
            Predicate { query: FindGiftCardQuery -> query.giftCardId == event.giftCardId },
            giftCard
        )

        queryUpdateEmitter.emit(
            FindAllGiftCardsQuery::class.java,
            Predicate { true },
            giftCard
        )
    }

    /**
     * Event handler for gift card redeemed events.
     */
    @EventHandler
    fun on(event: GiftCardRedeemedEvent, queryUpdateEmitter: QueryUpdateEmitter, @Timestamp timestamp: Instant) {
        giftCardRepository.findByGiftCardId(event.giftCardId).ifPresent { entity ->
            // Update remaining value and last modified date
            entity.remainingValue = entity.remainingValue!!.subtract(event.amount)
            entity.lastModifiedDate = OffsetDateTime.ofInstant(timestamp, ZoneOffset.UTC)
            giftCardRepository.save(entity)

            // Convert to summary for query updates - preserve existing encrypted data
            val updatedGiftCard = GiftCardSummary(
                giftCardId = entity.giftCardId!!,
                remainingValue = entity.remainingValue!!,
                initialValue = entity.initialValue!!,
                username = entity.username,
                owner = null, // owner not updated on redeem
                dateOfBirth = null, // dateOfBirth not updated on redeem
                randomNumber = null  // randomNumber not updated on redeem
            )

            queryUpdateEmitter.emit(
                FindGiftCardQuery::class.java,
                Predicate { query: FindGiftCardQuery -> query.giftCardId == event.giftCardId },
                updatedGiftCard
            )

            queryUpdateEmitter.emit(
                FindAllGiftCardsQuery::class.java,
                Predicate { true },
                updatedGiftCard
            )
        }
    }

    /**
     * Query handler for retrieving a specific gift card by ID.
     */
    @QueryHandler
    fun handle(query: FindGiftCardQuery): GiftCardSummary? {
        val optional = giftCardRepository.findByGiftCardId(query.giftCardId)
        return if (optional.isPresent) {
            val entity = optional.get()
            GiftCardSummary(
                giftCardId = entity.giftCardId!!,
                remainingValue = entity.remainingValue!!,
                initialValue = entity.initialValue!!,
                username = entity.username,
                owner = entity.owner,
                dateOfBirth = entity.dateOfBirth,
                randomNumber = entity.randomNumber
            )
        } else {
            null
        }
    }

    /**
     * Query handler for retrieving all gift cards in the system.
     */
    @QueryHandler
    fun handle(query: FindAllGiftCardsQuery): GiftCardSummaryList {
        // Load all entities and convert to summaries
        val summaries = giftCardRepository.findAll().map { entity ->
            GiftCardSummary(
                giftCardId = entity.giftCardId!!,
                remainingValue = entity.remainingValue!!,
                initialValue = entity.initialValue!!,
                username = entity.username,
                owner = entity.owner,
                dateOfBirth = entity.dateOfBirth,
                randomNumber = entity.randomNumber
            )
        }

        return GiftCardSummaryList(summaries)
    }

    @ResetHandler
    fun onReset() {
        logger.info("Resetting account repository")
        logger.info("Removing {} records.", giftCardRepository.count())
        giftCardRepository.deleteAllInBatch()
    }
}
