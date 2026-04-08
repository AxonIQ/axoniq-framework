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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Optional;

/**
 * Abstract implementation of the PropertyAccessStrategy that uses a no-arg, public method to access the property
 * value. The name of the method can be derived from the name of the property.
 *
 * @author Maxim Fedorov
 * @author Allard Buijze
 * @since 2.0
 */
public abstract class AbstractMethodPropertyAccessStrategy extends PropertyAccessStrategy {

    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    @Override
    public <T> Property<T> propertyFor(Class<? extends T> targetClass, @Nullable String property) {
        String methodName = getterName(property);
        Optional<Method> method = getMethod(targetClass, methodName);
        if (!method.isPresent()) {
            logger.debug("No method with name '{}' found in {} to use as property accessor. " +
                                 "Attempting to fall back to other strategies.",
                         methodName, targetClass.getName());
            return null;
        } else {
            return new MethodAccessedProperty<>(method.get(), property);
        }
    }

    private <T> Optional<Method> getMethod(Class<T> targetClass, String methodName) {
        return Arrays.stream(targetClass.getMethods())
                     .filter(method -> method.getName().equals(methodName))
                     .filter(method -> method.getParameterCount() == 0)
                     .filter(this::isNotReturningVoid)
                     .findFirst();
    }

    private boolean isNotReturningVoid(Method method) {
        boolean returnsVoid = method.getReturnType().equals(Void.TYPE);
        if (returnsVoid && logger.isDebugEnabled()) {
            logger.debug(
                    "Method with name '{}' in '{}' cannot be accepted as a property accessor, as it returns void",
                    method.getName(), method.getDeclaringClass().getName());
        }
        return !returnsVoid;
    }

    /**
     * Returns the name of the method that is used to access the property.
     *
     * @param property The property to access
     * @return the name of the method use as accessor
     */
    protected abstract String getterName(String property);
}
