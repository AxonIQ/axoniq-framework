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

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

/**
 * JPA Repository for GiftCardEntity persistence operations.
 *
 */
@Repository
public interface GiftCardRepository extends JpaRepository<GiftCardEntity, String> {

    /**
     * Find a gift card by its ID.
     *
     * @param giftCardId the gift card identifier
     * @return Optional containing the gift card entity if found
     */
    Optional<GiftCardEntity> findByGiftCardId(String giftCardId);
}
