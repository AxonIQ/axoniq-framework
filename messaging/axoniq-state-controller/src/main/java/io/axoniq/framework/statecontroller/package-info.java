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
 * Root package of the Axoniq State Controller module.
 * <p>
 * State Controllers group decisions for a functional scope. Each decision is a procedural function from a command
 * and a scoped slice of event history to a decision result (accept with events, or reject). The module is layered
 * into three production sub-packages with a strict, one-directional dependency rule:
 * {@code decisions} &rarr; {@code eventstream} &rarr; {@code conditions}. This separation keeps the condition
 * primitives extractable to a shared utility module the moment a second consumer needs them.
 */
@NullMarked
package io.axoniq.framework.statecontroller;

import org.jspecify.annotations.NullMarked;
