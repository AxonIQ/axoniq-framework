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
 * Business-first read surface of the State Controller module. Hosts the public {@code History} interface — a
 * decision's window onto recorded events, narrowed to a scope with {@code of(tagKey, tagValue)} (or the advanced
 * {@code matching(EventCriteria)}) and then read with a small vocabulary of plain value-returning methods
 * ({@code has}, {@code never}, {@code lastWas}, {@code latest}, {@code latestOf}, {@code first}, {@code count},
 * {@code total}, {@code entry}) — and the eager runtime behind it.
 * <p>
 * {@code RootHistory} is the unbound {@code History} injected into a decision; every read on it fails until the
 * scope is narrowed. {@code SourcedHistory} is the narrowed, eager implementation: on first read it sources the
 * scope's slice once via the active {@code EventStoreTransaction}, materializes it into a list, and answers all
 * subsequent reads from that snapshot. {@code HistoryFactory} mints and caches these per
 * {@link org.axonframework.messaging.core.unitofwork.ProcessingContext ProcessingContext}, mirroring the DCB
 * consistency boundary that {@code DefaultEventStoreTransaction} threads automatically.
 * <p>
 * Depends on Axon Framework's existing event and event-store types.
 */
@NullMarked
package io.axoniq.framework.statecontroller.history;

import org.jspecify.annotations.NullMarked;
