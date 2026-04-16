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
package io.axoniq.workflow.configuration;

import io.axoniq.workflow.dsl.api.AssociationsUtils;
import io.axoniq.workflow.runtime.api.annotation.OnCancellation;
import io.axoniq.workflow.runtime.api.annotation.OnFailure;
import io.axoniq.workflow.runtime.api.annotation.OnSuccess;
import io.axoniq.workflow.runtime.api.annotation.OnTimeout;
import io.axoniq.workflow.runtime.api.annotation.Workflow;
import io.axoniq.workflow.runtime.api.execution.context.EventCondition;
import io.axoniq.workflow.runtime.api.execution.context.EventConditions;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowIdProvider;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowStatusChangeListener;
import io.axoniq.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.workflow.runtime.association.ValueComparisonOperatorRegistry;
import io.axoniq.workflow.runtime.execution.DefaultEventNameCustomizer;
import io.axoniq.workflow.runtime.execution.PayloadPropertyWorkflowIdProvider;
import io.axoniq.workflow.runtime.util.WorkflowReflectionUtils;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.axonframework.common.ReflectionUtils;
import org.axonframework.common.StringUtils;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.configuration.ComponentBuilder;
import org.axonframework.conversion.Converter;
import org.axonframework.messaging.core.QualifiedName;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static io.axoniq.workflow.runtime.api.annotation.Workflow.*;
import static io.axoniq.workflow.runtime.api.execution.status.WorkflowStatus.*;
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

    /**
     * Method with workflow attributes.
     *
     * @param method              method.
     * @param attributes          attributes as map.
     * @param workflowContextType workflow context type.
     */
    public record MethodWithWorkflowAttributes(
            Method method,
            Map<String, Object> attributes,
            Class<? extends WorkflowContext> workflowContextType
    ) {

    }

    /**
     * Retrieve workflow methods.
     *
     * @param type                type to detect methods on.
     * @param workflowContextType workflow context type.
     * @return workflow methods.
     */
    @Nonnull
    public static <C extends WorkflowContext> Stream<MethodWithWorkflowAttributes> workflowMethods(
            @Nonnull Class<?> type,
            @Nonnull Class<C> workflowContextType) {
        Stream<Method> methodStream;
        try {
            methodStream = StreamSupport.stream(ReflectionUtils.methodsOf(type).spliterator(), false);
        } catch (Exception e) {
            methodStream = Arrays.stream(type.getMethods());
        }
        return methodStream
                .filter(hasContextParameter(workflowContextType)) // FIXME using parameter resolver
                .map(AutoDetectionUtils.annotatedMethods(Workflow.class))
                .filter(Objects::nonNull);
    }


    /**
     * Retrieves workflow lifecycle change listeners for this workflow.
     *
     * @param instance            instance to detect methods on.
     * @param workflowContextType workflow context type.
     * @param workflowName        name of the workflow.
     * @return map of lifecycle change listeners.
     */
    @Nonnull
    public static <C extends WorkflowContext> Map<WorkflowStatus, CompositeWorkflowStatusChangeListener> statusChangeListeners(
            @Nonnull Object instance,
            @Nonnull Class<C> workflowContextType,
            @Nonnull String workflowName) {

        var listeners = new ConcurrentHashMap<WorkflowStatus, CompositeWorkflowStatusChangeListener>();
        Arrays.stream(WorkflowStatus.values()).forEach(workflowStatus -> {
            listeners.put(workflowStatus, new CompositeWorkflowStatusChangeListener(workflowStatus));
        });
        var type = instance.getClass();

        var mc = ((Collection<Method>) ReflectionUtils.methodsOf(type));

        // TODO: consider registration based on workflow name?
        detectAndAddListener(mc,
                             OnCancellation.class,
                             workflowName,
                             instance,
                             workflowContextType,
                             CANCELLED,
                             listeners);
        detectAndAddListener(mc, OnTimeout.class, workflowName, instance, workflowContextType, TIMED_OUT, listeners);
        detectAndAddListener(mc, OnFailure.class, workflowName, instance, workflowContextType, FAILED, listeners);
        detectAndAddListener(mc, OnSuccess.class, workflowName, instance, workflowContextType, COMPLETED, listeners);

        return listeners;
    }

    private static <C extends WorkflowContext> void detectAndAddListener(
            @Nonnull Collection<Method> methodCandidates,
            @Nonnull Class<? extends Annotation> annotation,
            @Nonnull String workflowName,
            @Nonnull Object instance,
            @Nonnull Class<C> workflowContextType,
            @Nonnull WorkflowStatus status,
            @Nonnull ConcurrentHashMap<WorkflowStatus, CompositeWorkflowStatusChangeListener> listeners) {
        methodCandidates.stream()
                        .filter(hasParameterOfType(WorkflowStatus.class).and(hasContextParameter(workflowContextType)))
                        .map(AutoDetectionUtils.annotatedMethods(annotation))
                        .filter(Objects::nonNull)
                        .filter(mwa -> {
                            Object nameAttr = mwa.attributes.getOrDefault(ATTR_WORKFLOW_NAME, "");
                            return !(nameAttr instanceof String s) || s.isEmpty() || s.equals(workflowName);
                        })
                        .forEach(mwa -> {
                            listeners.get(status).addListener(
                                    new WorkflowStatusChangeListener() {
                                        @Override
                                        public <X extends WorkflowContext> void onWorkflowStatus(
                                                @Nonnull WorkflowStatus state, @Nonnull X context) {
                                            Method method = mwa.method();
                                            Class<?>[] parameterTypes = method.getParameterTypes();
                                            Object[] args = new Object[parameterTypes.length];
                                            for (int i = 0; i < parameterTypes.length; i++) {
                                                if (parameterTypes[i].isAssignableFrom(WorkflowStatus.class)) {
                                                    args[i] = state;
                                                } else if (WorkflowContext.class.isAssignableFrom(parameterTypes[i])) {
                                                    args[i] = context;
                                                } else if (parameterTypes[i].isInstance(instance)) {
                                                    args[i] = instance;
                                                } else {
                                                    Object wrap = wrapIfPossible(parameterTypes[i], context);
                                                    if (wrap != null) {
                                                        args[i] = wrap;
                                                    }
                                                }
                                            }
                                            WorkflowReflectionUtils.invoke(instance, method, args);
                                        }
                                    }
                            );
                        });
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
                    AssociationsUtils.parse(opRegistry, associationValues).build(c)
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
     * Creates a predicate to check if the method has at least one parameter of given type
     * or a parameter that can wrap it.
     *
     * @param expectedType type to check for.
     * @return predicate.
     */
    @Nonnull
    static Predicate<Method> hasContextParameter(@Nonnull Class<?> expectedType) {
        return m -> {
            var parameterTypes = m.getParameterTypes();
            for (var type : parameterTypes) {
                if (WorkflowContext.class.isAssignableFrom(type) || isWrapper(type)) {
                    return true;
                }
            }
            return false;
        };
    }

    /**
     * Checks if given type can wrap the expected type.
     *
     * @param type         type to check.
     * @param expectedType type to be wrapped.
     * @return true if it can.
     */
    static boolean canWrap(@Nonnull Class<?> type, @Nonnull Class<?> expectedType) {
        try {
            for (var constructor : type.getConstructors()) {
                if (constructor.getParameterCount() == 1) {
                    var parameterType = constructor.getParameterTypes()[0];
                    if (WorkflowContext.class.isAssignableFrom(parameterType)) {
                        return true;
                    }
                }
            }
        } catch (NoClassDefFoundError e) {
            return false;
        }
        return false;
    }

    /**
     * Wraps the context if possible.
     *
     * @param type    type to wrap into.
     * @param context context to wrap.
     * @return wrapped context or null.
     */
    @Nullable
    public static Object wrapIfPossible(@Nonnull Class<?> type, @Nullable WorkflowContext context) {
        if (context == null) {
            return null;
        }
        try {
            var constructor = type.getConstructor(context.getClass());
            return constructor.newInstance(context);
        } catch (NoSuchMethodException | InvocationTargetException | InstantiationException |
                 IllegalAccessException e) {
            // try with interface
            try {
                var constructor = type.getConstructor(WorkflowContext.class);
                return constructor.newInstance(context);
            } catch (NoSuchMethodException | InvocationTargetException | InstantiationException |
                     IllegalAccessException ex) {
                return null;
            }
        }
    }

    /**
     * Creates a predicate to check if the method has at least one parameter of given type.
     *
     * @param expectedType type to check for.
     * @return predicate.
     */
    @Nonnull
    static Predicate<Method> hasParameterOfType(@Nonnull Class<?> expectedType) {
        return m -> {
            var parameterTypes = m.getParameterTypes();
            return Arrays.stream(parameterTypes).anyMatch(expectedType::isAssignableFrom);
        };
    }

    /**
     * Creates a predicate to check if the parameter of the method, addressed by its index is assignable from given
     * type.
     *
     * @param expectedType   type to check for.
     * @param parameterIndex index of the parameter to check
     * @return predicate.
     */
    @Nonnull
    static Predicate<Method> parameterOfType(@Nonnull Class<?> expectedType, int parameterIndex) {
        return m -> {
            var parameterTypes = m.getParameterTypes();
            return parameterTypes.length > parameterIndex && expectedType.isAssignableFrom(
                    parameterTypes[parameterIndex]);
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
    static Function<Method, MethodWithWorkflowAttributes> annotatedMethods(
            @Nonnull Class<? extends Annotation> type) {
        return m -> {
            var annotations = findAnnotationAttributes(m, type);
            return annotations
                    .map(attributes -> new MethodWithWorkflowAttributes(m, attributes, findWorkflowContextType(m)))
                    .orElse(null);
        };
    }

    static Class<? extends WorkflowContext> findWorkflowContextType(@Nonnull Method method) {
        var parameterTypes = method.getParameterTypes();
        return Stream.of(parameterTypes)
                     .filter(type -> WorkflowContext.class.isAssignableFrom(type) || isWrapper(type))
                     .map(p -> {
                         if (WorkflowContext.class.isAssignableFrom(p)) {
                             //noinspection unchecked
                             return (Class<? extends WorkflowContext>) p;
                         } else {
                             // it is a wrapper, so we find the type it wraps
                             Class<?> wrapped = WorkflowContext.class;
                             for (var constructor : p.getConstructors()) {
                                 if (constructor.getParameterCount() == 1 && WorkflowContext.class.isAssignableFrom(
                                         constructor.getParameterTypes()[0])) {
                                     wrapped = constructor.getParameterTypes()[0];
                                     break;
                                 }
                             }
                             //noinspection unchecked
                             return (Class<? extends WorkflowContext>) wrapped;
                         }
                     })
                     .findFirst()
                     .orElseThrow(
                             () -> new IllegalArgumentException(
                                     "Method must have at least one parameter of type assignable to WorkflowContext")
                     );
    }

    static boolean isWrapper(@Nonnull Class<?> type) {
        try {
            return Arrays.stream(type.getConstructors())
                         .anyMatch(c -> c.getParameterCount() == 1 && WorkflowContext.class.isAssignableFrom(c.getParameterTypes()[0]));
        } catch (NoClassDefFoundError e) {
            return false;
        }
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
