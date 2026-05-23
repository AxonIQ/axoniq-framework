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
 * State-controller runtime of the State Controller module. Hosts the developer-facing surface for writing
 * decisions — {@code DecisionContext}, the {@code Decision} sealed result, and the {@code @StateController}
 * annotation — and will later host the lazy {@code EventStream} implementation and the DCB consistency wiring
 * that turns an {@code Accept} decision into a conditional append against the event store. Depends on both
 * {@link io.axoniq.framework.statecontroller.eventstream} and
 * {@link io.axoniq.framework.statecontroller.conditions}.
 */
@NullMarked
package io.axoniq.framework.statecontroller.decisions;

import org.jspecify.annotations.NullMarked;
