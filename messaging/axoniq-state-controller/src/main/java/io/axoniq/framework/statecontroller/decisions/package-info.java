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
 * decisions — {@link io.axoniq.framework.statecontroller.decisions.DecisionContext DecisionContext}, the
 * {@link io.axoniq.framework.statecontroller.decisions.Decision Decision} sealed result, the
 * {@link io.axoniq.framework.statecontroller.decisions.StateController @StateController} annotation, and the
 * declarative {@link io.axoniq.framework.statecontroller.decisions.StateControllerComponent StateControllerComponent}
 * — along with the runtime that wires them into Axon Framework: the
 * {@link io.axoniq.framework.statecontroller.decisions.DecisionContextParameterResolverFactory parameter resolver}
 * that injects {@code DecisionContext} into annotated methods and the
 * {@link io.axoniq.framework.statecontroller.decisions.StateControllerHandlerEnhancer handler enhancer} that
 * translates a returned {@code Decision} into event appends via the in-context
 * {@link org.axonframework.messaging.eventhandling.gateway.EventAppender EventAppender}.
 * <p>
 * The {@link io.axoniq.framework.statecontroller.eventstream.EventStream EventStream} implementation that each
 * decision body uses lives in {@link io.axoniq.framework.statecontroller.eventstream}; this package constructs
 * it via {@link io.axoniq.framework.statecontroller.eventstream.SourcedEventStream SourcedEventStream} from
 * {@code DecisionContext.scope(...)}.
 * <p>
 * Depends on both {@link io.axoniq.framework.statecontroller.eventstream} and
 * {@link io.axoniq.framework.statecontroller.conditions}; never the reverse.
 */
@NullMarked
package io.axoniq.framework.statecontroller.decisions;

import org.jspecify.annotations.NullMarked;
