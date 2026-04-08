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

package org.axonframework.common.property;

import org.jspecify.annotations.Nullable;

import java.lang.reflect.Field;

/**
 * Implementation of PropertyAccessStrategy that scans class hierarchy to get public field named "property"
 */
public class DirectPropertyAccessStrategy extends PropertyAccessStrategy {

	@Override
	protected int getPriority() {
		return -2048;
	}

	@Override
    @Nullable
	protected <T> Property<T> propertyFor(Class<? extends T> targetClass, @Nullable String property) {
		Field[] fields = targetClass.getFields();
		for(Field field : fields) {
			if (field.getName().equals(property)) {
				return new DirectlyAccessedProperty<>(field, property);
			}
		}
		return null;
	}
}
