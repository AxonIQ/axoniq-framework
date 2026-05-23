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
 * Event-aware layer of the State Controller module. Defines the {@code EventStream} surface — the fluent
 * {@code fold} builder ({@code FoldableCondition}) plus the typed sugar helpers ({@code contains},
 * {@code containsAnyOf}, {@code count}, {@code sum}, {@code latest}, {@code latestOf}, {@code latestMatch},
 * {@code first}, {@code firstOf}) — and the {@code EventCondition} subtype. Depends on
 * {@link io.axoniq.framework.statecontroller.conditions} and on Axon Framework's existing event types; must not
 * depend on {@link io.axoniq.framework.statecontroller.decisions}.
 */
@NullMarked
package io.axoniq.framework.statecontroller.eventstream;

import org.jspecify.annotations.NullMarked;
