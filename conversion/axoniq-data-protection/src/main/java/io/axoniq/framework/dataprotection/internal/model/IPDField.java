/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
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
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */
package io.axoniq.framework.dataprotection.internal.model;

import lombok.NonNull;
import lombok.Value;

import java.lang.reflect.Field;

/**
 * Model class representing an Indirect Personal Data field.
 *
 * @author Frans van Buul
 */
@Value
public class IPDField implements PDField, MapKeyField, MapValueField {
    @NonNull Class<?> clazz;
    @NonNull Field field;
    @NonNull String group;
    @NonNull String replacement;
    boolean reencrypt;
}
