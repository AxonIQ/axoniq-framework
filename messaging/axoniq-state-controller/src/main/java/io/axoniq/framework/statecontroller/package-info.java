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
 * Root package of the Axoniq State Controller module: the public surface of state-controlled command handling.
 * <p>
 * A state-controlled handler is a plain {@code @CommandHandler} method that receives a {@link History} and
 * returns an {@link Outcome} (or {@code CompletableFuture<Outcome>}) — no dedicated annotation exists; the
 * signature is the opt-in. Reads on a narrowed {@code History} declare
 * {@link io.axoniq.framework.statecontroller.conditions.Condition Conditions}; the first
 * {@link io.axoniq.framework.statecontroller.conditions.Condition#resolve() resolution} loads every declared
 * condition in one event-store read whose criteria double as the decision's Dynamic Consistency Boundary.
 * <p>
 * Sub-packages, layered one-directionally: {@code runtime} (internal wiring) &rarr; {@code decisions} (the
 * advanced condition-based surface) &rarr; {@code eventstream} &rarr; {@code conditions}. The condition
 * primitives stay extractable to a shared utility module the moment a second consumer needs them.
 */
@NullMarked
package io.axoniq.framework.statecontroller;

import org.jspecify.annotations.NullMarked;
