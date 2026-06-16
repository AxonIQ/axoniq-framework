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

package io.axoniq.framework.statecontroller.business.banking;

import org.axonframework.eventsourcing.annotation.EventTag;

import java.math.BigDecimal;

/**
 * Event: money was withdrawn from the account identified by {@code accountId}. The {@code accountId} is tagged
 * {@code account} so the withdrawal lands in that account's {@code history.of("account", id)} scope — including
 * the debit leg of a transfer, which carries the source account's id and so brings the source inside the
 * transfer decision's DCB boundary.
 */
public record MoneyWithdrawn(@EventTag(key = "account") String accountId, BigDecimal amount) {

}
