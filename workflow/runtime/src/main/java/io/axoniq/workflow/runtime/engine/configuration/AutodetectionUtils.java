package io.axoniq.workflow.runtime.engine.configuration;

import io.axoniq.workflow.runtime.api.Workflow;
import io.axoniq.workflow.runtime.api.WorkflowIdProvider;
import io.axoniq.workflow.runtime.engine.impl.DefaultEventNameCustomizer;
import jakarta.annotation.Nonnull;
import org.axonframework.common.StringUtils;
import org.testcontainers.shaded.org.yaml.snakeyaml.util.Tuple;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

import static io.axoniq.workflow.runtime.api.Workflow.*;
import static org.axonframework.common.annotation.AnnotationUtils.findAnnotationAttributes;

public class AutodetectionUtils {

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
