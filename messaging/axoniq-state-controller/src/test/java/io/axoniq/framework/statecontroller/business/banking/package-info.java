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

/**
 * Banking sample exercising the business-first {@code @Decide} / {@code History} API end to end against an
 * in-memory event store. The {@code Accounts} decisions are the ADR's "Single scope" and "Multiple scopes"
 * worked examples written verbatim:
 * <ul>
 *     <li>{@code withdraw} narrows one {@code account} scope, rejecting on a closed account or insufficient
 *         funds and otherwise emitting {@code MoneyWithdrawn};</li>
 *     <li>{@code transfer} narrows two {@code account} scopes in one decision — the DCB cross-entity payoff —
 *         debiting the source and crediting the target atomically under a consistency boundary that spans both
 *         accounts.</li>
 * </ul>
 * Every event tags its {@code accountId} as {@code account}, so each event lands in the relevant account's
 * scope; for a transfer the produced {@code MoneyWithdrawn}/{@code MoneyDeposited} carry the source and target
 * ids respectively, keeping both accounts inside the decision's DCB boundary.
 */
@NullMarked
package io.axoniq.framework.statecontroller.business.banking;

import org.jspecify.annotations.NullMarked;
