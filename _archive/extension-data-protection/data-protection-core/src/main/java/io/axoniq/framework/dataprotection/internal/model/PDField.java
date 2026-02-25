/*
 * Copyright (c) 2010-2025. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://lp.axoniq.io/axoniq-software-subscription-agreement-terms
 *
 *
 */
package io.axoniq.framework.dataprotection.internal.model;

import java.lang.reflect.Field;

/**
 * Interface for fields that are encrypted directly rather than deeply (both IPD and SPD).
 *
 * @author Frans van Buul
 */
public interface PDField {
    Class<?> getClazz();
    Field getField();
    String getGroup();
    String getReplacement();
}
