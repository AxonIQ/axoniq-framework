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

package io.axoniq.framework.dataprotection.internal.utils;

import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Field;
import java.lang.reflect.Member;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Utility class for working with Java Reflection API.
 *
 * @author Allard Buijze
 * @since 0.7
 */
public abstract class ReflectionUtils {

    /**
     * A map of Primitive types to their respective wrapper types.
     */
    private static final Map<Class<?>, Class<?>> primitiveWrapperTypeMap = new HashMap<>(8);

    static {
        primitiveWrapperTypeMap.put(boolean.class, Boolean.class);
        primitiveWrapperTypeMap.put(byte.class, Byte.class);
        primitiveWrapperTypeMap.put(char.class, Character.class);
        primitiveWrapperTypeMap.put(double.class, Double.class);
        primitiveWrapperTypeMap.put(float.class, Float.class);
        primitiveWrapperTypeMap.put(int.class, Integer.class);
        primitiveWrapperTypeMap.put(long.class, Long.class);
        primitiveWrapperTypeMap.put(short.class, Short.class);
    }

    private ReflectionUtils() {
        // utility class
    }

    /**
     * Returns the value of the given {@code field} in the given {@code object}. If necessary, the field is
     * made accessible, assuming the security manager allows it.
     *
     * @param field  The field containing the value
     * @param object The object to retrieve the field's value from
     * @param <R> The type of the field
     * @return the value of the {@code field} in the {@code object}
     * @throws IllegalStateException if the field is not accessible and the security manager doesn't allow it to be
     *                               made accessible
     */
    @SuppressWarnings("unchecked")
    public static <R> R getFieldValue(Field field, Object object) {
        ensureAccessible(field);
        try {
            return (R) field.get(object);
        } catch (IllegalArgumentException | IllegalAccessException ex) {
            throw new IllegalStateException("Unable to access field for getting.", ex);
        }
    }

    /**
     * Set the {@code field} of {@code object} to a certain {@code value}. If necessary, the field is made accessible,
     * assuming the security manager allows it.
     * <p>
     * This method supports setting final fields using Java's Unsafe API, which is necessary for
     * immutable objects like Java records.
     *
     * @param field The field to set {@code value} on
     * @param object The object to set the {@code value} on {@code field}
     * @param value The value to set on {@code field}
     * @param <T> The type of the {@code value}
     */
    public static <T> void setFieldValue(Field field, Object object, T value) {
        // Check if field is final - if so, use Unsafe API directly
        if (java.lang.reflect.Modifier.isFinal(field.getModifiers())) {
            try {
                setFinalFieldValue(field, object, value);
                return;
            } catch (Exception unsafeEx) {
                throw new IllegalStateException("Unable to access final field for setting.", unsafeEx);
            }
        }

        // For non-final fields, use regular reflection
        ensureAccessible(field);
        try {
            field.set(object, value);
        } catch (IllegalAccessException ex) {
            // Last resort: try Unsafe API as fallback
            try {
                setFinalFieldValue(field, object, value);
            } catch (Exception unsafeEx) {
                throw new IllegalStateException("Unable to access field for setting.", ex);
            }
        }
    }

    /**
     * Sets a final field value using Unsafe API. This is necessary for immutable objects like Java records.
     *
     * @param field The field to set
     * @param object The object containing the field
     * @param value The new value
     * @throws Exception if the field cannot be set
     */
    private static <T> void setFinalFieldValue(Field field, Object object, T value) throws Exception {
        Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
        java.lang.reflect.Field unsafeField = unsafeClass.getDeclaredField("theUnsafe");
        unsafeField.setAccessible(true);
        Object unsafe = unsafeField.get(null);

        long offset = (long) unsafeClass.getMethod("objectFieldOffset", Field.class).invoke(unsafe, field);

        // Put the value using appropriate unsafe method based on field type
        Class<?> fieldType = field.getType();
        if (fieldType.isPrimitive()) {
            if (fieldType == boolean.class) {
                unsafeClass.getMethod("putBoolean", Object.class, long.class, boolean.class).invoke(unsafe, object, offset, value);
            } else if (fieldType == byte.class) {
                unsafeClass.getMethod("putByte", Object.class, long.class, byte.class).invoke(unsafe, object, offset, value);
            } else if (fieldType == short.class) {
                unsafeClass.getMethod("putShort", Object.class, long.class, short.class).invoke(unsafe, object, offset, value);
            } else if (fieldType == char.class) {
                unsafeClass.getMethod("putChar", Object.class, long.class, char.class).invoke(unsafe, object, offset, value);
            } else if (fieldType == int.class) {
                unsafeClass.getMethod("putInt", Object.class, long.class, int.class).invoke(unsafe, object, offset, value);
            } else if (fieldType == long.class) {
                unsafeClass.getMethod("putLong", Object.class, long.class, long.class).invoke(unsafe, object, offset, value);
            } else if (fieldType == float.class) {
                unsafeClass.getMethod("putFloat", Object.class, long.class, float.class).invoke(unsafe, object, offset, value);
            } else if (fieldType == double.class) {
                unsafeClass.getMethod("putDouble", Object.class, long.class, double.class).invoke(unsafe, object, offset, value);
            }
        } else {
            // Reference type (Object)
            unsafeClass.getMethod("putObject", Object.class, long.class, Object.class).invoke(unsafe, object, offset, value);
        }
    }

    /**
     * Returns the class on which the method with name "{@code getter}" and parameters of type
     * {@code parameterTypes} is declared. The given {@code instanceClass} is the instance on which the
     * method cn be called. If the method is not available on the given {@code instanceClass}, {@code null}
     * is returned.
     *
     * @param instanceClass  The class on which to look for the method
     * @param methodName     The name of the method
     * @param parameterTypes The parameter types of the method
     * @return The class on which the method is declared, or {@code null} if not found
     */
    public static Class<?> declaringClass(Class<?> instanceClass, String methodName, Class<?>... parameterTypes) {
        try {
            return instanceClass.getMethod(methodName, parameterTypes).getDeclaringClass();
        } catch (NoSuchMethodException e) {
            return null;
        }
    }

    /**
     * Indicates whether the given class implements a customized equals method. This methods returns true if the
     * declaring type of the equals method is not {@code Object}.
     *
     * @param type The type to inspect
     * @return {@code true} if the given type overrides the equals method, otherwise {@code false}
     */
    public static boolean hasEqualsMethod(Class<?> type) {
        return !Object.class.equals(declaringClass(type, "equals", Object.class));
    }

    /**
     * Indicates whether the two given objects are <em>not the same</em>, override an equals method that indicates
     * they are <em>not equal</em>, or implements {@link Comparable} which indicates the two are not equal. If this
     * method cannot safely indicate two objects are not equal, it returns
     * {@code false}.
     *
     * @param value      One of the values to compare
     * @param otherValue other value to compare
     * @return {@code true} if these objects explicitly indicate they are not equal, {@code false} otherwise.
     */
    @SuppressWarnings("unchecked")
    public static boolean explicitlyUnequal(Object value, Object otherValue) {
        if (value == otherValue) { // NOSONAR (The == comparison is intended here)
            return false;
        } else if (value == null || otherValue == null) {
            return true;
        } else if (value instanceof Comparable) {
            return ((Comparable) value).compareTo(otherValue) != 0;
        } else if (hasEqualsMethod(value.getClass())) {
            return !value.equals(otherValue);
        }
        return false;
    }

    /**
     * Makes the given {@code member} accessible via reflection if it is not the case already.
     *
     * @param member The member (field, method, constructor, etc) to make accessible
     * @param <T>    The type of member to make accessible
     * @return the given {@code member}, for easier method chaining
     * @throws IllegalStateException if the member is not accessible and the security manager doesn't allow it to be
     *                               made accessible
     */
    public static <T extends AccessibleObject> T ensureAccessible(T member) {
        if (!isAccessible(member)) {
            try {
                member.setAccessible(true);
            } catch (SecurityException e) {
                throw new IllegalStateException(
                        "Failed to make member accessible: " + member, e
                );
            }
        }
        return member;
    }

    /**
     * Indicates whether the given {@code member} is accessible. It does so by checking whether the member is
     * non-final and public, or can be accessed via reflection.
     *
     * @param member The member (field, method, constructor, etc) to check for accessibility
     * @return {@code true} if the member is accessible, otherwise {@code false}.
     */
    public static boolean isAccessible(AccessibleObject member) {
        if (member instanceof Member m && isNonFinalPublicMember(m)) {
            return true;
        }
        try {
            return member.canAccess(null);
        } catch (IllegalArgumentException e) {
            // canAccess(null) throws for non-static members - they need setAccessible
            return false;
        }
    }

    /**
     * Checks whether the given {@code member} is public and non-final. These members do no need to be set
     * accessible using reflection.
     *
     * @param member The member to check
     * @return {@code true} if the member is public and non-final, otherwise {@code false}.
     * @see #isAccessible(java.lang.reflect.AccessibleObject)
     * @see #ensureAccessible(java.lang.reflect.AccessibleObject)
     */
    public static boolean isNonFinalPublicMember(Member member) {
        return (Modifier.isPublic(member.getModifiers())
                && Modifier.isPublic(member.getDeclaringClass().getModifiers())
                && !Modifier.isFinal(member.getModifiers()));
    }

    /**
     * Returns an {@link Iterable} of all the fields declared on the given class and its super classes. The iterator
     * will always return fields declared in a subtype before returning fields declared in a super type.
     *
     * @param clazz The class to return fields for
     * @return an {@code Iterable} providing access to all declared fields in the class hierarchy
     */
    public static Iterable<Field> fieldsOf(Class<?> clazz) {
        List<Field> fields = new LinkedList<>();
        Class<?> currentClazz = clazz;
        do {
            fields.addAll(Arrays.asList(currentClazz.getDeclaredFields()));
            currentClazz = currentClazz.getSuperclass();
        } while (currentClazz != null);
        return Collections.unmodifiableList(fields);
    }

    /**
     * Returns an {@link Iterable} of all the methods declared on the given class and its super classes. The iterator
     * will always return methods declared in a subtype before returning methods declared in a super type.
     *
     * @param clazz The class to return methods for
     * @return an {@code Iterable} providing access to all declared methods in the class hierarchy
     */
    public static Iterable<Method> methodsOf(Class<?> clazz) {
        List<Method> methods = new LinkedList<>();
        Class<?> currentClazz = clazz;
        do {
            methods.addAll(Arrays.asList(currentClazz.getDeclaredMethods()));
            addMethodsOnDeclaredInterfaces(currentClazz, methods);
            currentClazz = currentClazz.getSuperclass();
        } while (currentClazz != null);
        return Collections.unmodifiableList(methods);
    }

    /**
     * Returns the boxed wrapper type for the given {@code primitiveType}.
     *
     * @param primitiveType The primitive type to return boxed wrapper type for
     * @return the boxed wrapper type for the given {@code primitiveType}
     * @throws IllegalArgumentException will be thrown instead of returning null if no wrapper class was found.
     */
    public static Class<?> resolvePrimitiveWrapperType(Class<?> primitiveType) {
        Assert.notNull(primitiveType, () -> "primitiveType may not be null");
        Assert.isTrue(primitiveType.isPrimitive(), () -> "primitiveType is not actually primitive: " + primitiveType);

        Class<?> primitiveWrapperType = primitiveWrapperTypeMap.get(primitiveType);
        Assert.notNull(primitiveWrapperType, () -> "no wrapper found for primitiveType: " + primitiveType);
        return primitiveWrapperType;
    }

    private static void addMethodsOnDeclaredInterfaces(Class<?> currentClazz, List<Method> methods) {
        for (Class iface : currentClazz.getInterfaces()) {
            methods.addAll(Arrays.asList(iface.getDeclaredMethods()));
            addMethodsOnDeclaredInterfaces(iface, methods);
        }
    }

    /**
     * Indicates whether the given field has the "transient" modifier
     *
     * @param field the field to inspect
     * @return {@code true} if the field is marked transient, otherwise {@code false
     * }
     */
    public static boolean isTransient(Field field) {
        return Modifier.isTransient(field.getModifiers());
    }

    /**
     * Resolve a generic type parameter from a field declaration
     *
     * @param field            The field to find generic parameters for
     * @param genericTypeIndex The index of the type
     * @return an optional that contains the resolved type, if found
     */
    public static Optional<Class<?>> resolveGenericType(Field field, int genericTypeIndex) {
        final Type genericType = field.getGenericType();
        if (genericType == null
                || !(genericType instanceof ParameterizedType)
                || ((ParameterizedType) genericType).getActualTypeArguments().length <= genericTypeIndex) {
            return Optional.empty();
        }
        return Optional.of((Class<?>) ((ParameterizedType) genericType).getActualTypeArguments()[genericTypeIndex]);
    }
}
