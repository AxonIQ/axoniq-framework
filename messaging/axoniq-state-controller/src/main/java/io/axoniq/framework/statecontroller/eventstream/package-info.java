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
 * Event-aware layer of the State Controller module. Hosts both the public {@code EventStream} surface — the
 * fluent {@code fold} builder ({@code FoldableCondition}) plus the typed sugar helpers ({@code contains},
 * {@code containsAnyOf}, {@code count}, {@code sum}, {@code sumLong}, {@code latest}, {@code latestOf},
 * {@code latestMatch}, {@code first}, {@code firstOf}) and the {@code EventCondition} subtype — and the
 * sourced runtime implementation behind them: {@code SourcedEventStream} owns the per-scope seal-and-load
 * lifecycle and drives a single coordinated read against the event store on first force; source-side accumulators
 * ({@code ContainsAnyCondition}, {@code CountCondition}, {@code BigDecimalSumCondition},
 * {@code LongSumCondition}, {@code SourcedEventSelection}, {@code SourcedFoldableCondition}) observe events through
 * that read and produce typed values via per-condition {@code CompletableFuture}s.
 * <p>
 * {@code LateConditionException} signals the seal-lifecycle violation: declaring a new condition (or
 * registering a new event type) on a scope after one of its conditions has already been forced.
 * <p>
 * Depends on {@link io.axoniq.framework.statecontroller.conditions} and on Axon Framework's existing event
 * types; must not depend on {@link io.axoniq.framework.statecontroller.decisions}.
 */
@NullMarked
package io.axoniq.framework.statecontroller.eventstream;

import org.jspecify.annotations.NullMarked;
