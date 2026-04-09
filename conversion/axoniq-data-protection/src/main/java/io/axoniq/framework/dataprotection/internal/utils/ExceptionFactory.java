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

import com.google.protobuf.InvalidProtocolBufferException;
import io.axoniq.framework.dataprotection.api.ConfigurationException;
import io.axoniq.framework.dataprotection.api.DataException;
import io.axoniq.framework.dataprotection.api.Scope;
import io.axoniq.framework.dataprotection.cryptoengine.KeyType;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.Provider;
import java.security.UnrecoverableKeyException;
import java.sql.SQLException;
import java.text.MessageFormat;
import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;

/**
 * Factory class for creating standardized exceptions with error codes used throughout
 * the Data Protection Module.
 *
 * @author Frans van Buul
 */
public abstract class ExceptionFactory {

    private ExceptionFactory() {
        throw new Error("non-instantiable util class");
    }

    /* Error code scheme:
     *   ADPM-0000 internal exceptions (should never occur)
     *   ADPM-1000 platform exceptions (should never occur)
     *   ADPM-2000 platform warnings
     *   ADPM-4000 configuration exception
     *   ADPM-5000 keystore exception
     *   ADPM-6000 datastore exception
     */

    public static IllegalArgumentException missingOperation() {
        return new IllegalArgumentException("ADPM-0001. operation must be ENCRYPT or DECRYPT");
    }

    public static UnsupportedOperationException wrongTypeForIPD(Class<?> type) {
        return new UnsupportedOperationException(MessageFormat.format("ADPM-0002. while trying to encrypt IPD data, " +
                "an unsupported type {0} was found. This should never happen, since type checking takes place in an earlier " +
                "stage.", type));
    }

    public static UnsupportedOperationException wrongTypeForStorage(Class<?> type) {
        return new UnsupportedOperationException(MessageFormat.format("ADPM-0003. while trying to encrypt SPD data, " +
                "an unsupported storage type {0} was found. This should never happen, since type checking takes place in an earlier " +
                "stage.", type));
    }

    public static IllegalStateException unexpectedKeyType(KeyType type) {
        return new IllegalStateException(MessageFormat.format("ADPM-0004. unexpected KeyType {0}. This should " +
                "never happen, since this should be enforced at an earlier stage.", type));
    }

    public static UnsupportedOperationException unexpectedSizeChange() {
        return new UnsupportedOperationException("ADPM-0005. Unexpected size change when manipulation singleton or array-backed map or collection.");
    }

    public static IllegalStateException collectionUtilsMisconfig(Class<?> type) {
        return new IllegalStateException(MessageFormat.format("ADPM-0006. wrapper type misconfiguration for {0}", type));
    }

    public static IllegalStateException unexpectedTypeInModelInspector(Type type) {
        return new IllegalStateException(MessageFormat.format("ADPM-0007. encountered unexpected type {0} while inspecting models", type));
    }

    public static ConfigurationException forNoSuchAlg(NoSuchAlgorithmException ex, String algorithm) {
        return new ConfigurationException(MessageFormat.format("ADPM-1001. Unable to instantiate crypto algorithm {0}. All" +
                " algorithms used by the Axon Data Protection Module should normally be available on every Java platform.",
                algorithm), ex);
    }

    public static ConfigurationException forNoSuchPadding(NoSuchPaddingException ex, String padding) {
        return new ConfigurationException(MessageFormat.format("ADPM-1002. Unable to instantiate crypto padding {0}. The" +
                        " padding used by the Axon Data Protection Module should normally be available on every Java platform.",
                padding), ex);
    }

    public static ConfigurationException forIllegalBlockSizeEnc(IllegalBlockSizeException ex) {
        return new ConfigurationException("ADPM-1003. IllegalBlockSizeException during encryption. This" +
                " should never happen since padding is used.", ex);
    }

    public static ConfigurationException forBadPaddingEnc(BadPaddingException ex) {
        return new ConfigurationException("ADPM-1004. BadPaddingException during encryption. This" +
                " should never happen since padding is used.", ex);
    }

    public static ConfigurationException forJPAEntityInstantiation(Class<?> clazz, Constructor constructor, Exception ex) {
        return new ConfigurationException(MessageFormat.format("ADPM-1101. Unable to reflectively instantiate the JPA entity" +
                " class {0} using constructor {1}", clazz, constructor), ex);
    }

    public static ConfigurationException forJPANoConstructor(Class<?> clazz, Exception ex) {
        return new ConfigurationException(MessageFormat.format("ADPM-1102. Unable to reflectively find zero-arg constructor for JPA entity" +
                " class {0}", clazz), ex);
    }

    public static String unableToAnalyzeWrapper(Class<?> clazz) {
        return MessageFormat.format("ADPM-2001. Unable to analyze wrapper type {0} - this type will not be supported for @PersonalData encryption.", clazz);
    }

    public static ConfigurationException forNullKeyType() {
        return new ConfigurationException("ADPM-4001. KeyType argument is null.");
    }

    public static ConfigurationException noKeyForGroup(String group) {
        return new ConfigurationException(MessageFormat.format("ADPM-4002. While encrypting @PersonalData field with group <{0}>, " +
                "unable to find @DataSubjectId key in context with same group.", group));
    }

    public static ConfigurationException annSerializedAndOthers(Field field) {
        return new ConfigurationException(MessageFormat.format("ADPM-4101. On {0}, @SerializedPersonalData is " +
                "combined with other Axon Data Protection Module annotations. This is never allowed.", field));
    }

    public static ConfigurationException annMoreThan2OnMapField(Field field, String group) {
        return new ConfigurationException(MessageFormat.format("ADPM-4102. More than 2 Axon Data Protection Module " +
                "(Deep)PersonalData annotations are present on Map field {0} for group <{1}>. This is not allowed.", field, group));
    }

    public static ConfigurationException annMoreThan1OnNonMapField(Field field, String group) {
        return new ConfigurationException(MessageFormat.format("ADPM-4103. More than 1 Axon Data Protection Module " +
                "(Deep)PersonalData annotation is present on non-Map field {0} for group <{1}>. This is not allowed.", field, group));
    }

    public static ConfigurationException annScopeNotDefaultOnNonMapField(Field field) {
        return new ConfigurationException(MessageFormat.format("ADPM-4104. Axon Data Protection Module annotation " +
                "has scope other than DEFAULT on non-Map field {0}. This is not allowed.", field));
    }

    public static ConfigurationException annScopeDefaultOnMapField(Field field) {
        return new ConfigurationException(MessageFormat.format("ADPM-4105. Axon Data Protection Module annotation has " +
                "scope DEFAULT on Map field {0}. Scope must be explicitly set as KEY, VALUE or BOTH.", field));
    }

    public static ConfigurationException annConflictingDataSubjectId(Class<?> clazz, String group) {
        return new ConfigurationException(MessageFormat.format("ADPM-4106. Conflicting @DataSubjectId annotations. " +
                " 2 or more annotations are present on class {0} with group <{1}>", clazz, group));
    }

    public static ConfigurationException annWrongTypeForIPD(Scope scope, Field field, Type type) {
        return new ConfigurationException(MessageFormat.format("ADPM-4107. @PersonalData annotation " +
                "with scope {0} on field {1} is not allowed because type {2} doesn''t support it. ",
                scope, field, type));
    }

    public static ConfigurationException annWrongTypeForDPD(Scope scope, Field field, Type type) {
        return new ConfigurationException(MessageFormat.format("ADPM-4108. @DeepPersonalData annotation " +
                        "with scope {0} on field {1} is not allowed because type {2} doesn''t support it. ",
                scope, field, type));
    }

    public static ConfigurationException annStorageFieldMissing(String storageFieldName, Field dataField) {
        return new ConfigurationException(MessageFormat.format("ADPM-4109. Cannot find storage field {0} " +
                        "for data field {1}", storageFieldName, dataField));
    }

    public static ConfigurationException annStorageFieldWrongType(Field storageField, Field dataField) {
        return new ConfigurationException(MessageFormat.format("ADPM-4110. Storage field {0} " +
                "for data field {1} is not byte[] or String.", storageField, dataField));
    }

    public static ConfigurationException annSubjectIdOnMapMustBeKey(Field field, Scope scope) {
        return new ConfigurationException(MessageFormat.format("ADPM-4111. A @DataSubjectId annotation " +
                "is present on map field {0} with scope {1}. Only KEY scope is allowed.", field, scope));
    }

    public static ConfigurationException annMapOverlappingScope(Field field, Scope scope) {
        return new ConfigurationException(MessageFormat.format("ADPM-4112. On Map field {0}, two annotations " +
                "are present with overlapping scope {1}. (Note that this may include an annotation with BOTH scope.)",
                field, scope));
    }

    public static ConfigurationException annStorageFieldUsedMultipleTimes(Field field) {
        return new ConfigurationException(MessageFormat.format("ADPM-4113. Field {0} is used as a storage " +
                "field for @SerializedPersonalData more than once.", field));
    }

    public static ConfigurationException annSubjectAndOthers(Field field) {
        return new ConfigurationException(MessageFormat.format("ADPM-4114. On {0}, @DataSubjectId is " +
                "combined with other Axon Data Protection Module annotations on a non-Map field.", field));
    }

    public static ConfigurationException immutableCollection(Field field) {
        return new ConfigurationException(MessageFormat.format("ADPM-4201. On {0}, unable to make required modifications" +
                "to immutable Scala collection.", field));
    }

    public static ConfigurationException annMapOverlappingScope(Field field, Scope scope, String group) {
        return new ConfigurationException(MessageFormat.format("ADPM-4115. On Map field {0}, two annotations " +
                        "are present with overlapping scope {1} on group <{2}>. (Note that this may include an annotation with BOTH scope.)",
                field, scope, group));
    }

    /**
     * Creates a ConfigurationException for unknown serialized type name.
     *
     * @param typeName the fully qualified class name that was not found
     * @return a ConfigurationException with appropriate error message
     * @since 5.0.0
     */
    public static ConfigurationException unknownSerializedType(String typeName) {
        return new ConfigurationException(MessageFormat.format(
                "ADPM-4200. When decrypted a @SerializedData field, the type was not found" +
                "  on the classpath: {0}", typeName));
    }

    public static DataException forInvalidKey(InvalidKeyException ex) {
        return new DataException("ADPM-5001. A key turned out to be invalid. This is unexpected as keys are created within the " +
                "system. The key database may be corrupted.", ex);
    }

    public static DataException forKeyStoreException(KeyStoreException ex) {
        return new DataException("ADPM-5002. KeyStoreException while reading/writing key.", ex);
    }

    public static DataException forUnrecoverableKeyException(UnrecoverableKeyException ex) {
        return new DataException("ADPM-5003. UnrecoverableKeyException while retrieving key.", ex);
    }

    public static DataException forKeyStoreDeleteException(KeyStoreException ex) {
        return new DataException("ADPM-5004. KeyStoreException while deleting key.", ex);
    }

    public static ConfigurationException forUnableToAddProvider(Provider provider) {
        return new ConfigurationException(MessageFormat.format("ADPM-5005. Unable to add security provder {0}.", provider));
    }

    public static ConfigurationException forUnableToOpenKeyStore(Exception ex) {
        return new ConfigurationException("ADPM-5006. Unable to open keystore.", ex);
    }

    public static DataException forSQLException(SQLException ex) {
        return new DataException("ADPM-5010. SQL Exception.", ex);
    }

    public static DataException forInvalidIv(InvalidAlgorithmParameterException ex) {
        return new DataException("ADPM-6001. An initialization vector (IV) turned out to be invalid. This is unexpected as " +
                "IVs are created within the system. The data storage may be corrupted.", ex);
    }

    public static DataException forInvalidProtobuf(InvalidProtocolBufferException ex) {
        return new DataException("ADPM-6002. A protobuf message turned out to be invalid upon decryption. The data storage " +
                "may be corrupted.", ex);
    }

    public static DataException forIllegalBlockSizeDec(IllegalBlockSizeException ex) {
        return new DataException("ADPM-6003. IllegalBlockSizeException during decryption. The data storage " +
                "may be corrupted.", ex);
    }

    public static DataException forBadPaddingDec(BadPaddingException ex) {
        return new DataException("ADPM-6004. BadPaddingException during decryption. The data storage " +
                "may be corrupted.", ex);
    }


}
