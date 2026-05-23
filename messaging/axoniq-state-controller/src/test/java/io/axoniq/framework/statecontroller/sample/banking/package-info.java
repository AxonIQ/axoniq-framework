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
 * Banking sample domain exercising the State Controller API. Lives under {@code src/test} so it does not ship in
 * the production artifact and doubles as living documentation of how a decision body reads against the public
 * surface.
 */
@NullMarked
package io.axoniq.framework.statecontroller.sample.banking;

import org.jspecify.annotations.NullMarked;
