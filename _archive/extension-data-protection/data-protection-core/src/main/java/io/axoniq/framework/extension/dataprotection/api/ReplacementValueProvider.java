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
package io.axoniq.framework.extension.dataprotection.api;

import com.google.common.base.Defaults;

import java.lang.reflect.Field;
import java.lang.reflect.Type;

/**
 * Class that represents the replacement value mechanism. It takes care of setting a value in the class when
 * the decryption key is no longer there. The default behaviour of this class is very simple: for <code>String</code>
 * fields, it will use the value of the <code>replacement</code> attribute in the {@link PersonalData} annotation,
 * which is the empty <code>String</code> by default. In all other cases, it will be <code>null</code>. Applications
 * may define more fine grained behaviour by creating their own subclass and providing this when constructing a
 * {@link FieldEncrypter} or {@link FieldEncryptingConverter}.
 * <p>
 * A method of this class is invoked when a key is missing upon decryption. However, we also see an invocation upon
 * each encryption. This is to provide a partial, clear to be included in the storage. By default, nothing is stored.
 * This allows functionality such as keeping the year of a full date even if the date itself gets deleted, or keeping
 * the last 4 digits of a credit card number.
 *
 * @author Frans van Buul
 */
public class ReplacementValueProvider {

    /**
     * Invoked when a field cannot be decrypted because the key is missing. The default implementation returns
     * the value of <code>replacement</code> if <code>fieldType</code> equals <code>String</code>, and <code>null</code>
     * otherwise (or the default value if <code>fieldType</code> represents a primitive).
     *
     * @param clazz the class of the object on which the field was found. This is not necessarily the same as
     *              <code>field.getDeclaringClass()</code>, since the field may be declared on a superclass of the
     *              class of our current object
     * @param field the field we're decrypting
     * @param fieldType the generic type of value that we need. This is not always the same as <code>field.getGenericType()</code>,
     *                  since the module itself recurses into collections and arrays. So, if the field has type <code>List&lt;String&gt;</code>,
     *                  then the <code>fieldType</code> will be <code>String</code> rather than <code>List&lt;String&gt;</code>.
     * @param groupName the <code>groupName</code> from the {@link PersonalData} or {@link SerializedPersonalData} annotation
     * @param replacement the <code>replacement</code> from the {@link PersonalData} or {@link SerializedPersonalData} annotation
     * @param storedPartialValue the clear partial value previously stored with the encrypted value; may be <code>null</code>
     * @return the object to be used as field value; may be <code>null</code>
     */
    public Object replacementValue(Class<?> clazz, Field field, Type fieldType,
                                   String groupName, String replacement, byte[] storedPartialValue) {
        if(String.class.equals(fieldType)) {
            return replacement;
        } else {
            return Defaults.defaultValue(fieldType instanceof Class<?> ? ((Class<?>)fieldType) : null);
        }
    }

    /**
     * Invoked as part of the encryption process. Provides a partial, clear value in the form of a byte array to be
     * stored with the encrypted value. The default implementation always returns null.
     *
     * @param clazz the class of the object on which the field was found. This is not necessarily the same as
     *              <code>field.getDeclaringClass()</code>, since the field may be declared on a superclass of the
     *              class of our current object
     * @param field the field we're decrypting
     * @param fieldType the generic type of value that we need. This is not always the same as <code>field.getGenericType()</code>,
     *                  since the module itself recurses into collections and arrays. So, if the field has type <code>List&lt;String&gt;</code>,
     *                  then the <code>fieldType</code> will be <code>String</code> rather than <code>List&lt;String&gt;</code>.
     * @param groupName the <code>groupName</code> from the {@link PersonalData} or {@link SerializedPersonalData} annotation
     * @param replacement the <code>replacement</code> from the {@link PersonalData} or {@link SerializedPersonalData} annotation
     * @param inputValue the input value
     * @return the byte array to be stored; may be <code>null</code>
     */
    public byte[] partialValueForStorage(Class<?> clazz, Field field, Type fieldType,
                                         String groupName, String replacement, Object inputValue) {
        return null;
    }
}
