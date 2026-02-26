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
 *    https://lp.axoniq.io/axoniq-software-subscription-agreement-terms
 *
 *
 */
package io.axoniq.workflow.runtime.engine.configuration;

import io.axoniq.workflow.runtime.api.EventCondition;
import io.axoniq.workflow.runtime.api.WorkflowIdProvider;
import io.axoniq.workflow.runtime.api.annotation.Workflow;
import io.axoniq.workflow.runtime.engine.association.Associations;
import io.axoniq.workflow.runtime.engine.association.ValueComparisonOperatorRegistry;
import io.axoniq.workflow.runtime.engine.execution.EventConditions;
import io.axoniq.workflow.runtime.engine.impl.DefaultEventNameCustomizer;
import io.axoniq.workflow.runtime.engine.impl.PayloadPropertyWorkflowIdProvider;
import io.axoniq.workflow.runtime.engine.util.WorkflowReflectionUtils;
import jakarta.annotation.Nonnull;
import org.axonframework.common.StringUtils;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.configuration.ComponentBuilder;
import org.axonframework.conversion.Converter;
import org.axonframework.messaging.core.QualifiedName;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

import static io.axoniq.workflow.runtime.api.annotation.Workflow.*;
import static org.axonframework.common.annotation.AnnotationUtils.findAnnotationAttributes;

/**
 * Utilities for workflow auto-detection.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
@Internal
public class AutoDetectionUtils {

    private AutoDetectionUtils() {
        // hide
    }

    record MethodWithWorkflowAttributes(
            Method method,
            Map<String, Object> attributes
    ) {

    }

    /**
     * Constructs a component builder for event condition.
     *
     * @param attributes attributes parsed from method annotation.
     * @return component builder for event condition.
     */
    @Nonnull
    static ComponentBuilder<EventCondition> eventConditionComponentBuilder(@Nonnull Map<String, Object> attributes) {
        return c ->
        {
            var opRegistry = c.getComponent(ValueComparisonOperatorRegistry.class,
                                            ValueComparisonOperatorRegistry::new);
            var associationValues = (String[]) attributes.get(ATTR_START_ON_CONDITIONS);
            return EventConditions.fromQualifiedName(
                    new QualifiedName((String) attributes.get(ATTR_START_ON_EVENT)),
                    Associations.parse(opRegistry, associationValues).build(c)
            );
        };
    }


    /**
     * Constructs a component builder for workflow id provider.
     *
     * @param attributes attributes parsed from method annotation.
     * @return component builder for workflow id provider.
     */
    @Nonnull
    static ComponentBuilder<WorkflowIdProvider> workflowIdProviderComponentBuilder(
            @Nonnull Map<String, Object> attributes) {
        return c ->
                getIfNotDefault(attributes, ATTR_ID_PROPERTY_PROVIDER, PayloadPropertyWorkflowIdProvider.class)
                        .flatMap(WorkflowReflectionUtils::createDefaultInstance) // FIXME -> HACK -> Ask Steven
                        .orElseGet(
                                () -> new PayloadPropertyWorkflowIdProvider(c.getComponent(Converter.class),
                                                                            (String) attributes.get(
                                                                                    ATTR_ID_PROPERTY))
                        );
    }

    /**
     * Extracts default event name customizer.
     *
     * @param type       class containing the workflow.
     * @param attributes attributes parsed from method annotation.
     * @param <T>        type of workflow class.
     * @return default namespace.
     */
    @Nonnull
    static <T> DefaultEventNameCustomizer namespace(@Nonnull Class<T> type, @Nonnull Map<String, Object> attributes) {
        return DefaultEventNameCustomizer.Builder.namespace(
                getIfNotDefault(attributes, ATTR_WORKFLOW_NAMESPACE, "").orElse(
                        type.getPackageName()
                )
        );
    }

    /**
     * Extract workflow name.
     *
     * @param type       class containing the workflow.
     * @param attributes attributes parsed from method annotation.
     * @param method     annotated method.
     * @param <T>        type of workflow class.
     * @return workflow name.
     */
    @Nonnull
    static <T> String workflowName(@Nonnull Class<T> type, @Nonnull Map<String, Object> attributes,
                                   @Nonnull Method method) {
        return getIfNotDefault(attributes, ATTR_WORKFLOW_NAME, "").orElse(
                type.getSimpleName() + "#" + StringUtils.capitalize(method.getName())
        );
    }

    /**
     * Creates a predicate to check if the first parameter of the method is assignable from given type.
     *
     * @param expectedType type to check for.
     * @return predicate.
     */
    @Nonnull
    static Predicate<Method> firstParameterOfType(@Nonnull Class<?> expectedType) {
        return m -> {
            var parameterTypes = m.getParameterTypes();
            return parameterTypes.length > 0 && parameterTypes[0].isAssignableFrom(expectedType);
        };
    }

    /**
     * Method processor assigning to every method its workflow annotation attributes or null.
     *
     * @param type annotation class to read attributes for.
     * @return a function which applied to a method either returns a tuple of method to annotations or
     * <code>null</code>.
     */
    @Nonnull
    static Function<Method, MethodWithWorkflowAttributes> extractAnnotatedMethods(
            @Nonnull Class<? extends Annotation> type) {
        return m -> {
            var annotations = findAnnotationAttributes(m, type);
            return annotations.map(attributes -> new MethodWithWorkflowAttributes(m, attributes)).orElse(null);
        };
    }

    /**
     * Validates attributes set via {@link Workflow} annotation (or its meta).
     *
     * @param attributes attributes read from annotation.
     * @param method     method the annotation was read of.
     * @param type       class the method was present from which the annotation was read of.
     */
    public static void validateAttributes(@Nonnull Map<String, Object> attributes,
                                          @Nonnull Class<?> type,
                                          @Nonnull Method method) {
        if (!attributes.containsKey(ATTR_ID_PROPERTY_PROVIDER) || WorkflowIdProvider.class.equals(attributes.get(
                ATTR_ID_PROPERTY_PROVIDER))) {
            if (!attributes.containsKey(ATTR_ID_PROPERTY)
                    || "".equals(attributes.get(ATTR_ID_PROPERTY))) {
                throw new IllegalArgumentException(
                        "Either " + ATTR_ID_PROPERTY + " or " + ATTR_ID_PROPERTY_PROVIDER + " must be specified, "
                                + " but none was specified on annotation of  " + type.getName() + "#"
                                + method.getName());
            }
        }
    }

    /**
     * Retrieves an optional value if it is not equals to the default provided.
     *
     * @param attributes    attributes with values.
     * @param attributeName name of the attribute to get the value.
     * @param defaultValue  default value.
     * @param <T>           value type.
     * @return optional with value if present and not equals to given, empty otherwise.
     */
    @Nonnull
    public static <T> Optional<T> getIfNotDefault(
            @Nonnull Map<String, Object> attributes,
            @Nonnull String attributeName,
            @Nonnull T defaultValue) {
        //noinspection unchecked
        return Optional.of(
                (T) attributes.get(attributeName)
        ).flatMap(o -> {
            if (defaultValue.equals(o)) {
                return Optional.empty();
            } else {
                return Optional.of(o);
            }
        });
    }
}
