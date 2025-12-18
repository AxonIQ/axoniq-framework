/*
 * Copyright (c) 2010-2025. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may obtain a copy of the License at:
 *
 *    https://lp.axoniq.io/axoniq-software-subscription-agreement-terms
 *
 *
 */

package io.axoniq.dataprotection.sample.giftcard.query

/**
 * Wrapper class for a collection of gift card summaries.
 *
 * @param giftCards the list of gift card summaries (never null, may be empty)
 * @author Stefan Mirkovic
 */
data class GiftCardSummaryList(val giftCards: List<GiftCardSummary>)
