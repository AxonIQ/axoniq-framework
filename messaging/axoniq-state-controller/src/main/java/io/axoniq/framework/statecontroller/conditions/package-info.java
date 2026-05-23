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
 * Pure value and composition layer of the State Controller module. Houses {@code Condition<T>} and its specialized
 * subtypes ({@code BooleanCondition}, {@code NumericCondition}, {@code OptionalCondition},
 * {@code CollectionCondition}) together with the {@code MatchBuilder} flow. This package must not depend on any
 * other sub-package of {@code io.axoniq.framework.statecontroller}; that constraint is what keeps it extractable
 * to a shared utility module the moment a second consumer needs it.
 */
@NullMarked
package io.axoniq.framework.statecontroller.conditions;

import org.jspecify.annotations.NullMarked;
