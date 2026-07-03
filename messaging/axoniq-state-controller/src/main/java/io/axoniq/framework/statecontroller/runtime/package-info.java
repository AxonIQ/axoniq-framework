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
 * Runtime wiring of the State Controller: the per-command {@code HistorySession}, the
 * {@link io.axoniq.framework.statecontroller.History History} implementation over the lazy condition engine, the
 * handler enhancer that recognizes state-controlled {@code @CommandHandler} methods by their
 * {@link io.axoniq.framework.statecontroller.Outcome Outcome} signature, and the outcome dispatch (append,
 * rejection, and DCB coverage guard).
 * <p>
 * Everything in this package is {@link org.axonframework.common.annotation.Internal @Internal}: it is created by
 * the framework's ServiceLoader-discovered enhancer and parameter resolvers, never by user code.
 */
@NullMarked
package io.axoniq.framework.statecontroller.runtime;

import org.jspecify.annotations.NullMarked;
