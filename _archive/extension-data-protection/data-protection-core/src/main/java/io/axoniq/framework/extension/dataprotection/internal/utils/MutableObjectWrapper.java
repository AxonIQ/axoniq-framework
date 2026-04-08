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

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

/**
 * Wrapper around an object that handles both mutable and immutable (record) objects.
 * For mutable objects, it directly modifies fields. For immutable objects (records),
 * it collects modifications and creates a new instance when needed.
 *
 * @author Frans van Buul
 */
public class MutableObjectWrapper {

    private final Object originalObject;
    private final boolean isRecord;
    private final Map<String, Object> modifications;
    private Object resultObject;

    public MutableObjectWrapper(Object object) {
        this.originalObject = object;
        this.isRecord = RecordUtils.isRecord(object.getClass());
        this.modifications = isRecord ? new HashMap<>() : null;
        this.resultObject = object;
    }

    /**
     * Gets the value of a field from the object.
     *
     * @param field the field to get
     * @param <T> the field value type
     * @return the field value
     */
    public <T> T getFieldValue(Field field) {
        // If we have a modification for this field, return it
        if (isRecord && modifications.containsKey(field.getName())) {
            return (T) modifications.get(field.getName());
        }
        // Otherwise get from the object
        return ReflectionUtils.getFieldValue(field, resultObject);
    }

    /**
     * Sets the value of a field on the object.
     * For records, this collects the modification. For regular objects, it sets directly.
     *
     * @param field the field to set
     * @param value the new value
     * @param <T> the field value type
     */
    public <T> void setFieldValue(Field field, T value) {
        if (isRecord) {
            // For records, collect the modification
            modifications.put(field.getName(), value);
        } else {
            // For regular objects, set directly
            ReflectionUtils.setFieldValue(field, resultObject, value);
        }
    }

    /**
     * Returns true if this wrapper contains a record (immutable object).
     *
     * @return true if the object is a record
     */
    public boolean isRecord() {
        return isRecord;
    }

    /**
     * Returns true if any modifications have been made.
     *
     * @return true if modifications exist
     */
    public boolean hasModifications() {
        return isRecord && !modifications.isEmpty();
    }

    /**
     * Gets the result object. For records, this creates a new instance with all modifications applied.
     * For regular objects, this returns the same object that was passed in.
     *
     * @return the result object
     * @throws Exception if a new record instance cannot be created
     */
    public Object getResult() throws Exception {
        if (isRecord && !modifications.isEmpty()) {
            // Extract all current field values from original object
            Map<String, Object> allFieldValues = RecordUtils.extractFieldValues(originalObject);
            // Apply modifications
            allFieldValues.putAll(modifications);
            // Create new instance
            resultObject = RecordUtils.createRecordInstance(originalObject.getClass(), allFieldValues);
            // Clear modifications since we've applied them
            modifications.clear();
        }
        return resultObject;
    }

    /**
     * Gets the wrapped object (without applying modifications).
     *
     * @return the wrapped object
     */
    public Object getObject() {
        return resultObject;
    }
}
