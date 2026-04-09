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
package io.axoniq.framework.extension.dataprotection.internal.utils;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.RecordComponent;
import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for working with Java records.
 * Records are immutable and cannot be mutated using reflection, so we need to create
 * new instances with modified field values.
 *
 * @author Frans van Buul
 */
public abstract class RecordUtils {

    private RecordUtils() {
        // utility class
    }

    /**
     * Checks if the given class is a Java record.
     *
     * @param clazz the class to check
     * @return true if the class is a record, false otherwise
     */
    public static boolean isRecord(Class<?> clazz) {
        return clazz.isRecord();
    }

    /**
     * Creates a new instance of a record with modified field values.
     * This method finds the canonical constructor and invokes it with the provided field values.
     *
     * @param recordClass the record class
     * @param fieldValues map of field names to their new values
     * @param <T> the record type
     * @return a new instance of the record with modified field values
     * @throws Exception if the record cannot be instantiated
     */
    @SuppressWarnings("unchecked")
    public static <T> T createRecordInstance(Class<T> recordClass, Map<String, Object> fieldValues) throws Exception {
        if (!recordClass.isRecord()) {
            throw new IllegalArgumentException("Class " + recordClass.getName() + " is not a record");
        }

        // Get record components (the canonical constructor parameters)
        RecordComponent[] components = recordClass.getRecordComponents();

        // Prepare constructor parameter types and values
        Class<?>[] parameterTypes = new Class<?>[components.length];
        Object[] parameterValues = new Object[components.length];

        for (int i = 0; i < components.length; i++) {
            RecordComponent component = components[i];
            String name = component.getName();
            parameterTypes[i] = component.getType();

            // Use provided value if available, otherwise get from original object
            if (fieldValues.containsKey(name)) {
                parameterValues[i] = fieldValues.get(name);
            } else {
                throw new IllegalArgumentException("Missing value for record component: " + name);
            }
        }

        // Find and invoke the canonical constructor
        Constructor<T> constructor = recordClass.getDeclaredConstructor(parameterTypes);
        constructor.setAccessible(true);
        return constructor.newInstance(parameterValues);
    }

    /**
     * Creates a new instance of a record with one field modified.
     *
     * @param original the original record instance
     * @param fieldName the name of the field to modify
     * @param newValue the new value for the field
     * @param <T> the record type
     * @return a new instance of the record with the modified field
     * @throws Exception if the record cannot be instantiated
     */
    @SuppressWarnings("unchecked")
    public static <T> T withModifiedField(T original, String fieldName, Object newValue) throws Exception {
        Class<T> recordClass = (Class<T>) original.getClass();

        // Extract all current field values
        Map<String, Object> fieldValues = extractFieldValues(original);

        // Modify the specified field
        fieldValues.put(fieldName, newValue);

        // Create new instance
        return createRecordInstance(recordClass, fieldValues);
    }

    /**
     * Creates a new instance of a record with multiple fields modified.
     *
     * @param original the original record instance
     * @param modifications map of field names to their new values
     * @param <T> the record type
     * @return a new instance of the record with modified fields
     * @throws Exception if the record cannot be instantiated
     */
    @SuppressWarnings("unchecked")
    public static <T> T withModifiedFields(T original, Map<String, Object> modifications) throws Exception {
        Class<T> recordClass = (Class<T>) original.getClass();

        // Extract all current field values
        Map<String, Object> fieldValues = extractFieldValues(original);

        // Apply modifications
        fieldValues.putAll(modifications);

        // Create new instance
        return createRecordInstance(recordClass, fieldValues);
    }

    /**
     * Extracts all field values from a record instance.
     *
     * @param record the record instance
     * @param <T> the record type
     * @return a map of field names to their current values
     * @throws Exception if fields cannot be accessed
     */
    public static <T> Map<String, Object> extractFieldValues(T record) throws Exception {
        Class<?> recordClass = record.getClass();
        if (!recordClass.isRecord()) {
            throw new IllegalArgumentException("Object is not a record: " + recordClass.getName());
        }

        Map<String, Object> fieldValues = new HashMap<>();
        RecordComponent[] components = recordClass.getRecordComponents();

        for (RecordComponent component : components) {
            String name = component.getName();
            Field field = recordClass.getDeclaredField(name);
            field.setAccessible(true);
            Object value = field.get(record);
            fieldValues.put(name, value);
        }

        return fieldValues;
    }

    /**
     * Gets the value of a specific field from a record.
     *
     * @param record the record instance
     * @param fieldName the name of the field
     * @param <T> the field value type
     * @return the field value
     * @throws Exception if the field cannot be accessed
     */
    @SuppressWarnings("unchecked")
    public static <T> T getFieldValue(Object record, String fieldName) throws Exception {
        Class<?> recordClass = record.getClass();
        Field field = recordClass.getDeclaredField(fieldName);
        field.setAccessible(true);
        return (T) field.get(record);
    }
}
