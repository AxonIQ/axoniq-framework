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
package io.axoniq.framework.workflow.configuration;

import io.axoniq.framework.workflow.runtime.api.annotation.Workflow;
import io.axoniq.framework.workflow.runtime.api.annotation.WorkflowStatusChangedHandler;
import io.axoniq.framework.workflow.runtime.api.execution.context.EventCondition;
import io.axoniq.framework.workflow.runtime.api.execution.context.EventConditions;
import io.axoniq.framework.workflow.runtime.api.execution.context.Version;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowIdProvider;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowStatusChangeListener;
import io.axoniq.framework.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.framework.workflow.runtime.association.Associations;
import io.axoniq.framework.workflow.runtime.association.ValueComparisonOperatorRegistry;
import io.axoniq.framework.workflow.runtime.execution.DefaultEventNameCustomizer;
import io.axoniq.framework.workflow.runtime.execution.PayloadPropertyWorkflowIdProvider;
import io.axoniq.framework.workflow.runtime.util.FutureResolver;
import io.axoniq.framework.workflow.runtime.util.WorkflowReflectionUtils;
import org.axonframework.common.Assert;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.common.ReflectionUtils;
import org.axonframework.common.StringUtils;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.configuration.ComponentBuilder;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.annotation.AnnotatedHandlerInspector;
import org.axonframework.messaging.core.annotation.MessageHandlingMember;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.jspecify.annotations.Nullable;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static io.axoniq.framework.workflow.configuration.WorkflowMethodParameterResolverFactory.*;
import static io.axoniq.framework.workflow.runtime.api.annotation.Workflow.*;
import static org.axonframework.common.ObjectUtils.getNonEmptyOrDefault;
import static org.axonframework.common.ObjectUtils.getOrDefault;
import static org.axonframework.common.annotation.AnnotationUtils.findAnnotationAttributes;

/**
 * Utilities for workflow auto-detection.
 *
 * @author Simon Zambrovski
 * @author Steven van Beelen
 * @since 5.4.0
 * TODO #494 - This class deserves further clean-up.
 */
@Internal
public final class AutoDetectionUtils {

    private AutoDetectionUtils() {
        // hide
    }

    /**
     * Method with workflow attributes.
     *
     * @param method              method.
     * @param attributes          attributes as map.
     * @param workflowContextType workflow context type derived from the method's parameters, or {@code null} if none of
     *                            them are assignable to (or wrap) {@link WorkflowContext}.
     */
    public record MethodWithWorkflowAttributes(
            Method method,
            Map<String, @Nullable Object> attributes,
            @Nullable Class<? extends WorkflowContext> workflowContextType
    ) {

    }

    /**
     * Retrieve workflow methods.
     *
     * @param type type to detect methods on.
     * @return workflow methods.
     */
    public static Stream<MethodWithWorkflowAttributes> workflowMethods(Class<?> type) {
        return StreamSupport.stream(ReflectionUtils.methodsOf(type).spliterator(), false)
                            .map(AutoDetectionUtils.annotatedMethods(Workflow.class))
                            .filter(Objects::nonNull);
    }

    /**
     * Retrieves {@link WorkflowStatusChangeListener workflow lifecycle change listeners} for the workflow as contained
     * in the given {@code instance}.
     *
     * @param instance     instance to detect {@link WorkflowStatusChangeListener workflow lifecycle change listener}
     *                     methods on
     * @param workflowName name of the workflow
     * @param inspector    inspector holding the enhanced {@link MessageHandlingMember}s built for {@code instance}'s
     *                     class
     * @return unmodifiable map of {@link WorkflowStatusChangeListener lifecycle change listeners}
     */
    public static Map<WorkflowStatus, WorkflowStatusChangeListener> statusChangeListeners(
            @Nullable Object instance,
            String workflowName,
            AnnotatedHandlerInspector<Object> inspector
    ) {
        ConcurrentHashMap<WorkflowStatus, CompositeWorkflowStatusChangeListener> listeners = new ConcurrentHashMap<>();
        Arrays.stream(WorkflowStatus.values())
              .forEach(workflowStatus -> listeners.put(
                      workflowStatus,
                      new CompositeWorkflowStatusChangeListener(workflowStatus)
              ));

        if (instance == null) {
            return Collections.unmodifiableMap(listeners);
        }
        Class<?> type = instance.getClass();
        Collection<Method> mc = (Collection<Method>) ReflectionUtils.methodsOf(type);

        // TODO: consider registration based on workflow name?
        detectAndAddListener(mc,
                             WorkflowStatusChangedHandler.class,
                             workflowName,
                             instance,
                             inspector,
                             listeners);

        return Collections.unmodifiableMap(listeners);
    }

    private static void detectAndAddListener(
            Collection<Method> methodCandidates,
            Class<? extends Annotation> annotation,
            String workflowName,
            Object instance,
            AnnotatedHandlerInspector<Object> inspector,
            ConcurrentHashMap<WorkflowStatus, CompositeWorkflowStatusChangeListener> listeners
    ) {
        methodCandidates.stream()
                        .map(AutoDetectionUtils.annotatedMethods(annotation))
                        .filter(Objects::nonNull)
                        .filter(mwa -> {
                            Object nameAttr = mwa.attributes.getOrDefault(ATTR_WORKFLOW_NAME, "");
                            return !(nameAttr instanceof String s) || s.isEmpty() || s.equals(workflowName);
                        })
                        .forEach(mwa -> {
                            WorkflowStatus status = (WorkflowStatus) mwa.attributes.get(ATTR_WORKFLOW_STATUS);
                            MessageHandlingMember<Object> member = AutoDetectingWorkflowBuilder.findMember(
                                    inspector, instance, EventMessage.class, mwa.method()
                            );
                            listeners.get(status).addListener((changedStatus, context, event, processingContext) -> {
                                ProcessingContext contextWithResources = processingContext
                                        .withResource(WORKFLOW_CONTEXT_RESOURCE_KEY, context)
                                        .withResource(WORKFLOW_STATUS_RESOURCE_KEY, changedStatus)
                                        .withResource(WORKFLOW_INSTANCE_RESOURCE_KEY, instance);
                                CompletableFuture<?> future =
                                        inspector.chainedInterceptor(instance.getClass())
                                                 .handle(event, contextWithResources, instance, member)
                                                 .first()
                                                 .asCompletableFuture();
                                FutureResolver.resolve(processingContext, future);
                            });
                        });
    }

    /**
     * Constructs a component builder for event condition.
     *
     * @param attributes attributes parsed from method annotation.
     * @return component builder for event condition.
     */
    static ComponentBuilder<EventCondition> eventConditionComponentBuilder(Map<String, @Nullable Object> attributes) {
        return c -> {
            ValueComparisonOperatorRegistry opRegistry = c.getComponent(ValueComparisonOperatorRegistry.class,
                                                                        ValueComparisonOperatorRegistry::new);
            String[] associationValues = (String[]) attributes.get(ATTR_START_ON_CONDITIONS);

            QualifiedName eventName;
            String eventNameString = getNonEmptyOrDefault((String) attributes.get(ATTR_START_ON_EVENT_NAME), null);
            if (eventNameString == null) {
                Class<?> eventClass = getOrDefault((Class<?>) attributes.get(ATTR_START_ON_EVENT_CLASS), Void.class);
                eventName = c.getComponent(MessageTypeResolver.class)
                             .resolveOrThrow(eventClass)
                             .qualifiedName();
            } else {
                eventName = new QualifiedName(eventNameString);
            }
            return EventConditions.fromQualifiedName(
                    eventName,
                    Associations.parse(opRegistry, associationValues)
            );
        };
    }


    /**
     * Constructs a component builder for workflow id provider.
     *
     * @param attributes attributes parsed from method annotation.
     * @return component builder for workflow id provider.
     */
    static ComponentBuilder<WorkflowIdProvider> workflowIdProviderComponentBuilder(
            Map<String, @Nullable Object> attributes
    ) {
        return c -> {
            @SuppressWarnings("unchecked")
            Class<? extends WorkflowIdProvider> providerClass = getOrDefault(
                    (Class<? extends WorkflowIdProvider>) attributes.get(ATTR_ID_PROPERTY_PROVIDER),
                    PayloadPropertyWorkflowIdProvider.class
            );
            if (providerClass.isAssignableFrom(PayloadPropertyWorkflowIdProvider.class)) {
                return new PayloadPropertyWorkflowIdProvider((String) attributes.get(ATTR_ID_PROPERTY));
            } else {
                return resolveWorkflowIdProvider(c, providerClass);
            }
        };
    }

    /**
     * Resolves an explicitly named {@link WorkflowIdProvider}, preferring a real Axon component the user registered in
     * the {@link Configuration} over reflective instantiation, and failing loudly instead of silently falling back to a
     * different provider when neither succeeds.
     *
     * @param configuration configuration to resolve a registered component from.
     * @param providerClass the explicitly named {@link WorkflowIdProvider} type.
     * @return the resolved provider instance.
     */
    private static <T extends WorkflowIdProvider> T resolveWorkflowIdProvider(
            Configuration configuration, Class<T> providerClass) {
        return configuration.getOptionalComponent(providerClass)
                            .or(() -> WorkflowReflectionUtils.createDefaultInstance(providerClass))
                            .orElseThrow(() -> new AxonConfigurationException(
                                    "Could not resolve WorkflowIdProvider of type " + providerClass.getName()
                                            + ": it is not registered as a Configuration component, and has no "
                                            + "accessible no-arg constructor."
                            ));
    }

    /**
     * Extracts default event name customizer.
     *
     * @param type       class containing the workflow.
     * @param attributes attributes parsed from method annotation.
     * @param <T>        type of workflow class.
     * @return default namespace.
     */
    static <T> DefaultEventNameCustomizer namespace(Class<T> type, Map<String, @Nullable Object> attributes) {
        return DefaultEventNameCustomizer.Builder.namespace(
                getNonEmptyOrDefault((String) attributes.get(ATTR_WORKFLOW_NAMESPACE), type.getPackageName())
        );
    }

    /**
     * Extract workflow name.
     *
     * @param type       class containing the workflow
     * @param attributes attributes parsed from method annotation
     * @param method     annotated method
     * @param <T>        type of workflow class
     * @return workflow name
     */
    static <T> String workflowName(Class<T> type,
                                   Map<String, @Nullable Object> attributes,
                                   Method method) {
        return getNonEmptyOrDefault((String) attributes.get(ATTR_WORKFLOW_NAME),
                                    type.getSimpleName() + "#" + StringUtils.capitalize(method.getName()));
    }

    /**
     * Extract workflow version. Falls back to {@link Version#DEFAULT_VERSION}
     * ({@code "0.0.1"}) when the annotation does not specify one.
     *
     * @param attributes attributes parsed from method annotation
     * @return workflow version, validated as a semver string
     */
    static String workflowVersion(Map<String, @Nullable Object> attributes) {
        String version = getNonEmptyOrDefault((String) attributes.get(ATTR_WORKFLOW_VERSION),
                                              Version.DEFAULT_VERSION);
        Version.validate(version);
        return version;
    }

    /**
     * Wraps the context if possible.
     *
     * @param type    type to wrap into
     * @param context context to wrap
     * @return wrapped context or null
     */
    @Nullable
    public static Object wrapIfPossible(Class<?> type, @Nullable WorkflowContext context) {
        if (context == null) {
            return null;
        }
        try {
            Constructor<?> constructor = type.getConstructor(context.getClass());
            return constructor.newInstance(context);
        } catch (NoSuchMethodException | InvocationTargetException | InstantiationException |
                 IllegalAccessException e) {
            // try with interface
            try {
                Constructor<?> constructor = type.getConstructor(WorkflowContext.class);
                return constructor.newInstance(context);
            } catch (NoSuchMethodException | InvocationTargetException | InstantiationException |
                     IllegalAccessException ex) {
                return null;
            }
        }
    }

    /**
     * Method processor assigning to every method its workflow annotation attributes or null.
     *
     * @param type annotation class to read attributes for.
     * @return a function which applied to a method either returns a tuple of method to annotations or
     * <code>null</code>.
     */
    static Function<Method, MethodWithWorkflowAttributes> annotatedMethods(Class<? extends Annotation> type) {
        return m -> findAnnotationAttributes(m, type)
                .map(attributes -> new MethodWithWorkflowAttributes(m, attributes, findWorkflowContextType(m)))
                .orElse(null);
    }

    @Nullable
    static Class<? extends WorkflowContext> findWorkflowContextType(Method method) {
        Class<?>[] parameterTypes = method.getParameterTypes();
        return Stream.of(parameterTypes)
                     .filter(type -> WorkflowContext.class.isAssignableFrom(type) || isWorkflowContextWrapper(type))
                     .map(p -> {
                         if (WorkflowContext.class.isAssignableFrom(p)) {
                             //noinspection unchecked
                             return (Class<? extends WorkflowContext>) p;
                         } else {
                             // it is a wrapper, so we find the type it wraps
                             Class<?> wrapped = WorkflowContext.class;
                             for (Constructor<?> constructor : p.getConstructors()) {
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
                     .orElse(null);
    }

    static boolean isWorkflowContextWrapper(Class<?> type) {
        try {
            return Arrays.stream(type.getConstructors())
                         .anyMatch(c -> c.getParameterCount() == 1
                                 && WorkflowContext.class.isAssignableFrom(c.getParameterTypes()[0]));
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
    public static void validateAttributes(Map<String, @Nullable Object> attributes,
                                          Class<?> type,
                                          Method method) {
        boolean hasIdProviderSpecifiedCorrectly =
                attributes.containsKey(ATTR_ID_PROPERTY_PROVIDER) && !WorkflowIdProvider.class.equals(
                        attributes.get(ATTR_ID_PROPERTY_PROVIDER));
        boolean hasIdPropertySpecifiedCorrectly =
                attributes.containsKey(ATTR_ID_PROPERTY) && !"".equals(attributes.get(ATTR_ID_PROPERTY));
        Assert.isFalse(
                !hasIdProviderSpecifiedCorrectly && !hasIdPropertySpecifiedCorrectly,
                () -> "Either " + ATTR_ID_PROPERTY + " or " + ATTR_ID_PROPERTY_PROVIDER + " must be specified, "
                        + " but none was specified on annotation of  " + type.getName() + "#" + method.getName()
        );

        boolean hasStartOnEventClassSpecifiedCorrectly =
                attributes.containsKey(ATTR_START_ON_EVENT_CLASS) && !Void.class.equals(attributes.get(
                        ATTR_START_ON_EVENT_CLASS));
        boolean hasStartOnEventNameSpecifiedCorrectly =
                attributes.containsKey(ATTR_START_ON_EVENT_NAME) && !"".equals(
                        attributes.get(ATTR_START_ON_EVENT_NAME));
        Assert.isFalse(
                !hasStartOnEventClassSpecifiedCorrectly && !hasStartOnEventNameSpecifiedCorrectly,
                () -> "Either " + ATTR_START_ON_EVENT_NAME + " or " + ATTR_START_ON_EVENT_CLASS
                        + " must be specified, " + " but none was specified on annotation of  " + type.getName()
                        + "#" + method.getName()
        );
        Assert.isFalse(
                hasStartOnEventClassSpecifiedCorrectly && hasStartOnEventNameSpecifiedCorrectly,
                () -> "Either " + ATTR_START_ON_EVENT_NAME + " or " + ATTR_START_ON_EVENT_CLASS
                        + ", but not both must be specified, " + " but both were specified on annotation of  "
                        + type.getName() + "#" + method.getName()
        );
    }
}
