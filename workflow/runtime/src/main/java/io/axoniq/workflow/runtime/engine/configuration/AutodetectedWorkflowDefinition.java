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
import io.axoniq.workflow.runtime.api.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.Workflow;
import io.axoniq.workflow.runtime.api.WorkflowContext;
import io.axoniq.workflow.runtime.api.WorkflowDefinition;
import io.axoniq.workflow.runtime.api.WorkflowIdProvider;
import io.axoniq.workflow.runtime.engine.impl.DefaultEventNameCustomizer;
import io.axoniq.workflow.runtime.engine.impl.PayloadPropertyWorkflowIdProvider;
import io.axoniq.workflow.runtime.engine.util.WorkflowReflectionUtils;
import jakarta.annotation.Nonnull;
import org.axonframework.common.ReflectionUtils;
import org.axonframework.common.StringUtils;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.configuration.ComponentBuilder;
import org.axonframework.conversion.Converter;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.QualifiedName;
import org.slf4j.LoggerFactory;
import org.testcontainers.shaded.org.yaml.snakeyaml.util.Tuple;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

import static io.axoniq.workflow.runtime.api.Workflow.*;
import static io.axoniq.workflow.runtime.engine.util.WorkflowReflectionUtils.createDefaultInstance;
import static org.axonframework.common.annotation.AnnotationUtils.findAnnotationAttributes;

/**
 * Auto-detected workflow definition.
 *
 * @param name               name of the workflow.
 * @param startCondition     start condition of the workflow.
 * @param workflowDefinition workflow definition.
 * @param workflowIdProvider workflow id provider.
 * @param <C>                type of the workflow context.
 * @author Simon Zambrovski
 * @since 1.0.0
 */
@Internal record AutodetectedWorkflowDefinition<C extends WorkflowContext>(
        @Nonnull String name,
        @Nonnull ComponentBuilder<EventCondition> startCondition,
        @Nonnull ComponentBuilder<WorkflowDefinition<C>> workflowDefinition,
        @Nonnull ComponentBuilder<WorkflowIdProvider> workflowIdProvider,
        @Nonnull EventNameCustomizer eventNameCustomizer
) {

    /**
     * Creates a list of auto-detected definitions, scanning a specified class.
     *
     * @param autodetectionType   class to scan.
     * @param workflowContextType type of workflow.
     * @param <T>                 type of class to autodetect.
     * @param <C>                 type of workflow context.
     * @return list of autodetected workflow definitions.
     */
    static <T, C extends WorkflowContext> List<AutodetectedWorkflowDefinition<C>> fromClass(
            @Nonnull Class<T> autodetectionType,
            @Nonnull Class<C> workflowContextType
    ) {

        var methodCandidates = ((Collection<Method>) ReflectionUtils.methodsOf(autodetectionType));
        return methodCandidates
                .stream()
                .filter(firstParameterOfType(workflowContextType))
                .map(extractAnnotatedMethods(Workflow.class))
                .filter(Objects::nonNull)
                .map(t -> {

                    Map<String, Object> attributes = t._2();
                    Method method = t._1();

                    validateAttributes(attributes, autodetectionType, method);

                    var workflowName = getWorkflowName(autodetectionType, attributes, method);
                    var namespaceCustomizer = getNamespace(autodetectionType, attributes);

                    ComponentBuilder<WorkflowDefinition<C>> workflowDefinitionComponentBuilder = c -> {

                        // FIXME -> HACK -> Ask Steven
                        var instance = createDefaultInstance(autodetectionType).orElseThrow(() -> new IllegalArgumentException(
                                "Could not instantiate " + autodetectionType.getName()));

                        return workflowContext -> {
                            try {
                                method.invoke(instance, workflowContext);
                            } catch (InvocationTargetException | IllegalAccessException e) {
                                // FIXME -> this will catch any exception. this
                                LoggerFactory.getLogger("Autodetecter").error("Error 0", e);
                                throw new RuntimeException(e.getCause());
                            }
                        };
                    };

                    ComponentBuilder<EventCondition> eventConditionBuilder = c ->
                            new EventCondition(
                                    getIfNotDefault(attributes, ATTR_START_ON, Void.class)
                                            .flatMap(triggerType -> c.getComponent(MessageTypeResolver.class)
                                                                     .resolve(triggerType)
                                                                     .map(MessageType::qualifiedName)
                                            ).orElseGet(() -> new QualifiedName((String) attributes.get(
                                                    ATTR_START_ON_QUALIFIED_NAME))
                                            ),
                                    (e) -> true
                            ); // FIXME enrich with associations as soon as available, see #5


                    ComponentBuilder<WorkflowIdProvider> associationProviderComponentBuilder = c ->
                            getIfNotDefault(attributes,
                                            ATTR_ID_PROPERTY_PROVIDER,
                                            PayloadPropertyWorkflowIdProvider.class)
                                    .flatMap(WorkflowReflectionUtils::createDefaultInstance) // FIXME -> HACK -> Ask Steven
                                    .orElseGet(() -> new PayloadPropertyWorkflowIdProvider(c.getComponent(Converter.class),
                                                                                           (String) attributes.get(
                                                                                                   ATTR_ID_PROPERTY))
                                    );


                    return new AutodetectedWorkflowDefinition<>(
                            workflowName,
                            eventConditionBuilder,
                            workflowDefinitionComponentBuilder,
                            associationProviderComponentBuilder,
                            namespaceCustomizer
                    );
                }).toList();
    }

    static <T> DefaultEventNameCustomizer getNamespace(Class<T> autodetectionType, Map<String, Object> attributes) {
        return DefaultEventNameCustomizer.Builder.namespace(
                getIfNotDefault(attributes, ATTR_WORKFLOW_NAMESPACE, "").orElse(
                        autodetectionType.getPackageName()
                )
        );
    }

    static <T> String getWorkflowName(Class<T> autodetectionType, Map<String, Object> attributes, Method method) {
        return getIfNotDefault(attributes, ATTR_WORKFLOW_NAME, "").orElse(
                autodetectionType.getSimpleName() + "#" + StringUtils.capitalize(method.getName())
        );
    }

    /**
     * Creates a predicate to check if the first parameter of the method is assignable from given type.
     *
     * @param expectedType type to check for.
     * @return predicate.
     */
    public static Predicate<Method> firstParameterOfType(Class<?> expectedType) {
        return m -> {
            var parameterTypes = m.getParameterTypes();
            return parameterTypes.length > 0 && parameterTypes[0].isAssignableFrom(expectedType);
        };
    }

    /**
     * Method processor assigning to every method its workflow annotation attributes or null.
     *
     * @param annotationClass annotation class to read attributes for.
     * @return a function which applied to a method either returns a tuple of method to annotations or
     * <code>null</code>.
     */
    public static Function<Method, Tuple<Method, Map<String, Object>>> extractAnnotatedMethods(
            Class<? extends Annotation> annotationClass) {
        return m -> {
            var annotations = findAnnotationAttributes(m, annotationClass);
            return annotations.map(attributes -> new Tuple<>(m, attributes)).orElse(null);
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
        if (!attributes.containsKey(ATTR_START_ON) || Void.class.equals(attributes.get(ATTR_START_ON))) {
            if (!attributes.containsKey(ATTR_START_ON_QUALIFIED_NAME)
                    || "".equals(attributes.get(ATTR_START_ON_QUALIFIED_NAME))) {
                throw new IllegalArgumentException(
                        "Either " + ATTR_START_ON + " or " + ATTR_START_ON_QUALIFIED_NAME
                                + " attribute must be specified, "
                                + " but none was specified on annotation of  " + type.getName() + "#"
                                + method.getName());
            }
        }
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