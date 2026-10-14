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
 * An Axon Framework 4 {@code XStreamSerializer}-compatible {@link org.axonframework.conversion.Converter}, kept only to
 * drain saga state, deadlines and scheduled events that an Axon Framework 4 node serialized with XStream.
 */
@NullMarked
package org.axonframework.conversion.xstream;

import org.jspecify.annotations.NullMarked;
