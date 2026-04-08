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

package io.axoniq.framework.extension.dataprotection.sample.giftcard.query;

import io.axoniq.framework.extension.dataprotection.sample.giftcard.event.GiftCardIssuedEvent;
import io.axoniq.framework.extension.dataprotection.sample.giftcard.event.GiftCardRedeemedEvent;
import org.axonframework.messaging.eventhandling.annotation.EventHandler;
import org.axonframework.messaging.eventhandling.annotation.Timestamp;
import org.axonframework.messaging.eventhandling.replay.annotation.ResetHandler;
import org.axonframework.messaging.queryhandling.QueryUpdateEmitter;
import org.axonframework.messaging.queryhandling.annotation.QueryHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.lang.invoke.MethodHandles;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

/**
 * Event-driven projection that maintains a read model of gift card data.
 *
 */
@Component
public class GiftCardProjection {

    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());


    /**
     * JPA repository for persisting gift card entities to PostgreSQL.
     */
    private final GiftCardRepository giftCardRepository;

    public GiftCardProjection(GiftCardRepository giftCardRepository) {
        this.giftCardRepository = giftCardRepository;

    }

    /**
     * Event handler for gift card issued events.
     */
    @EventHandler
    public void on(GiftCardIssuedEvent event, QueryUpdateEmitter queryUpdateEmitter, @Timestamp Instant timestamp) {
        // Create and persist entity
        GiftCardEntity entity = new GiftCardEntity(
                event.giftCardId(),
                event.amount(),
                event.amount(),
                event.username(),
                event.owner(),
                event.dateOfBirth(),
                event.n(),
                OffsetDateTime.ofInstant(timestamp, ZoneOffset.UTC)
        );
        giftCardRepository.save(entity);

        // Convert to summary for query updates
        GiftCardSummary giftCard = new GiftCardSummary(
                entity.getGiftCardId(),
                entity.getRemainingValue(),
                entity.getInitialValue(),
                event.username(),
                event.owner(),
                event.dateOfBirth(),
                event.n()
        );

        queryUpdateEmitter.emit(FindGiftCardQuery.class,
                query -> query.giftCardId().equals(event.giftCardId()),
                giftCard);

        queryUpdateEmitter.emit(FindAllGiftCardsQuery.class,
                query -> true,
                giftCard);
    }

    /**
     * Event handler for gift card redeemed events.
     */
    @EventHandler
    public void on(GiftCardRedeemedEvent event, QueryUpdateEmitter queryUpdateEmitter, @Timestamp Instant timestamp) {
        giftCardRepository.findByGiftCardId(event.giftCardId()).ifPresent(entity -> {
            // Update remaining value and last modified date
            entity.setRemainingValue(entity.getRemainingValue().subtract(event.amount()));
            entity.setLastModifiedDate(OffsetDateTime.ofInstant(timestamp, ZoneOffset.UTC));
            giftCardRepository.save(entity);

            // Convert to summary for query updates - preserve existing encrypted data
            GiftCardSummary updatedGiftCard = new GiftCardSummary(
                    entity.getGiftCardId(),
                    entity.getRemainingValue(),
                    entity.getInitialValue(),
                    entity.getUsername(),
                    null,  // owner not updated on redeem
                    null,  // dateOfBirth not updated on redeem
                    null   // randomNumber not updated on redeem
            );

            queryUpdateEmitter.emit(FindGiftCardQuery.class,
                    query -> query.giftCardId().equals(event.giftCardId()),
                    updatedGiftCard);

            queryUpdateEmitter.emit(FindAllGiftCardsQuery.class,
                    query -> true,
                    updatedGiftCard);
        });
    }

    /**
     * Query handler for retrieving a specific gift card by ID.
     */
    @QueryHandler
    public GiftCardSummary handle(FindGiftCardQuery query) {
        return giftCardRepository.findByGiftCardId(query.giftCardId())
                .map(entity -> {
                    // Convert entity to summary
                    GiftCardSummary summary = new GiftCardSummary(
                            entity.getGiftCardId(),
                            entity.getRemainingValue(),
                            entity.getInitialValue(),
                            entity.getUsername(),
                            entity.getOwner(),
                            entity.getDateOfBirth(),
                            entity.getRandomNumber()
                    );
                    return summary;
                })
                .orElse(null);
    }

    /**
     * Query handler for retrieving all gift cards in the system.
     */
    @QueryHandler
    public GiftCardSummaryList handle(FindAllGiftCardsQuery query) {
        // Load all entities and convert to summaries
        var summaries = giftCardRepository.findAll().stream()
                .map(entity -> {
                    // Convert entity to summary
                    GiftCardSummary summary = new GiftCardSummary(
                            entity.getGiftCardId(),
                            entity.getRemainingValue(),
                            entity.getInitialValue(),
                            entity.getUsername(),
                            entity.getOwner(),
                            entity.getDateOfBirth(),
                            entity.getRandomNumber()
                    );
                    return summary;
                })
                .toList();

        return new GiftCardSummaryList(summaries);
    }

    @ResetHandler
    public void onReset() {
        logger.info("Resetting account repository");
        logger.info("Removing {} records.", giftCardRepository.count());
        giftCardRepository.deleteAllInBatch();
    }
}
