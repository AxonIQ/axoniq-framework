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

package org.axonframework.messaging.core.annotation;

import org.jspecify.annotations.Nullable;
import org.axonframework.common.Assert;
import org.axonframework.common.ReflectionUtils;
import org.axonframework.common.annotation.AnnotationUtils;

import java.lang.annotation.Annotation;
import java.lang.reflect.Executable;
import java.lang.reflect.Parameter;

/**
 * ParameterResolverFactory that will supply a parameter resolver when a matching parameter annotation is paired
 * with a suitable type of parameter.
 * <p>
 * Handling is in place to ensure that primitive parameter types will be resolved correctly from their respective
 * wrapper types.
 *
 * @param <A> The type of annotation to check for
 * @param <P> The type the parameter needs to be assignable from.
 * @author Mark Ingram
 * @since 2.1.0
 */
public abstract class AbstractAnnotatedParameterResolverFactory<A extends Annotation, P>
        implements ParameterResolverFactory {

    private final Class<A> annotationType;
    private final Class<P> declaredParameterType;

    /**
     * Initialize a ParameterResolverFactory instance that resolves parameters of type
     * {@code declaredParameterType} annotated with the given {@code annotationType}.
     *
     * @param annotationType        the type of annotation that a prospective parameter should declare
     * @param declaredParameterType the type that the parameter value should be assignable to
     */
    protected AbstractAnnotatedParameterResolverFactory(Class<A> annotationType, Class<P> declaredParameterType) {
        Assert.notNull(annotationType, () -> "annotationType may not be null");
        Assert.notNull(declaredParameterType, () -> "declaredParameterType may not be null");
        this.annotationType = annotationType;
        this.declaredParameterType = declaredParameterType;
    }

    /**
     * @return the parameter resolver that is supplied when a matching parameter is located
     */
    protected abstract ParameterResolver<P> getResolver();

    @Nullable
    @Override
    public ParameterResolver<P> createInstance(Executable executable, Parameter[] parameters, int parameterIndex) {
        if (AnnotationUtils.isAnnotationPresent(parameters[parameterIndex], annotationType)) {
            Class<?> parameterType = parameters[parameterIndex].getType();
            if (parameterType.isAssignableFrom(declaredParameterType)) {
                return getResolver();
            }

            //a 2nd chance to resolve if the parameter is primitive but its boxed wrapper type is assignable
            if (parameterType.isPrimitive()
                    && ReflectionUtils.resolvePrimitiveWrapperType(parameterType)
                    .isAssignableFrom(declaredParameterType)) {
                return getResolver();
            }
        }

        return null;
    }
}
