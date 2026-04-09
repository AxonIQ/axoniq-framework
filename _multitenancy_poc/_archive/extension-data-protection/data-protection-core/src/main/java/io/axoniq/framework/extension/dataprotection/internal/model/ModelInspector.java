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
package io.axoniq.framework.extension.dataprotection.internal.model;

import com.googlecode.gentyref.GenericTypeReflector;
import io.axoniq.framework.extension.dataprotection.api.DataSubjectId;
import io.axoniq.framework.extension.dataprotection.api.DataSubjectIdContainer;
import io.axoniq.framework.extension.dataprotection.api.DeepPersonalData;
import io.axoniq.framework.extension.dataprotection.api.DeepPersonalDataContainer;
import io.axoniq.framework.extension.dataprotection.api.PersonalData;
import io.axoniq.framework.extension.dataprotection.api.PersonalDataContainer;
import io.axoniq.framework.extension.dataprotection.api.PersonalDataType;
import io.axoniq.framework.extension.dataprotection.api.Scope;
import io.axoniq.framework.extension.dataprotection.api.SerializedPersonalData;
import io.axoniq.framework.extension.dataprotection.internal.utils.AnnotationUtils;
import io.axoniq.framework.extension.dataprotection.internal.utils.ExceptionFactory;
import io.axoniq.framework.extension.dataprotection.internal.utils.ReflectionUtils;
import lombok.NonNull;
import lombok.Value;

import java.lang.reflect.Field;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.WildcardType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.axoniq.framework.extension.dataprotection.internal.utils.ScalaDetector.isScalaPresent;
import static java.util.stream.StreamSupport.stream;

/**
 * Inspector class that analyzes classes for personal data annotations and builds
 * field encryption models.
 *
 * @author Frans van Buul
 */
public class ModelInspector {

    public FieldEncryptionModel inspect(Class<?> clazz) {
        verifyAnnotationCount(clazz);
        return new FieldEncryptionModel(
                getDataSubjIdFields(clazz),
                getIPDFields(clazz),
                getDPDFields(clazz),
                getSPDFields(clazz),
                getMPDFields(clazz)
        );
    }

    private void verifyAnnotationCount(Class<?> clazz) {
        for (Field field : ReflectionUtils.fieldsOf(clazz)) {
            int countSubject = getDataSubjectIdAnn(field).size();
            int countPersonalData = getPersonalDataAnn(field).size();
            Map<String, List<PersonalData>> personalDataPerGroup = getPersonalDataAnn(field)
                    .stream().collect(Collectors.groupingBy(PersonalData::group));
            int countDeepPersonalData = getDeepPersonalDataAnn(field).size();
            int countData = countPersonalData + countDeepPersonalData;
            int countSerialized = getSerializedPersonalDataAnn(field).size();
            int countNonSubject = countData + countSerialized;
            int countNonSerialized = countSubject + countData;
            if (countNonSerialized > 0 && countSerialized > 0) {
                throw ExceptionFactory.annSerializedAndOthers(field);
            }
            if (Map.class.isAssignableFrom(field.getType())) {
                for (String group : personalDataPerGroup.keySet()) {
                    if (personalDataPerGroup.get(group).size() > 2) {
                        throw ExceptionFactory.annMoreThan2OnMapField(field, group);
                    }
                }
            } else {
                if (countSubject > 0 && countNonSubject > 0) {
                    throw ExceptionFactory.annSubjectAndOthers(field);
                }
                for (String group : personalDataPerGroup.keySet()) {
                    if (personalDataPerGroup.get(group).size() > 1) {
                        throw ExceptionFactory.annMoreThan1OnNonMapField(field, group);
                    }
                }
            }
        }
    }

    @Value
    private static class DataSubjIdOccurrence {

        @NonNull
        Class<?> clazz;
        @NonNull
        String group;
    }

    private List<DataSubjIdField> getDataSubjIdFields(Class<?> clazz) {
        List<DataSubjIdField> fields = new ArrayList<>();
        /* Important fact of the fieldsOf method: "The iterator will always return fields declared in a subtype
           before returning fields declared in a super type." */
        Set<DataSubjIdOccurrence> occurrences = new HashSet<>();
        for (Field field : ReflectionUtils.fieldsOf(clazz)) {
            if (!Map.class.isAssignableFrom(field.getType())
                    && !(isScalaPresent() && scala.collection.Map.class.isAssignableFrom(field.getType()))) {
                for (DataSubjectId dataSubjectId : getDataSubjectIdAnn(field)) {
                    if (dataSubjectId.scope() != Scope.DEFAULT) {
                        throw ExceptionFactory.annScopeNotDefaultOnNonMapField(field);
                    }
                    ReflectionUtils.ensureAccessible(field);
                    DataSubjIdOccurrence occurrence = new DataSubjIdOccurrence(field.getDeclaringClass(),
                                                                               dataSubjectId.group());
                    if (occurrences.contains(occurrence)) {
                        throw ExceptionFactory.annConflictingDataSubjectId(occurrence.clazz, occurrence.group);
                    } else {
                        occurrences.add(occurrence);
                    }
                    fields.add(new DataSubjIdField(field, dataSubjectId.group(), dataSubjectId.prefix()));
                }
            }
        }
        /* The ObjectDPDProcessor will process dataSubjIdFields in the order of our list. To make sure that
         * subclass annotations override superclass annotations, they must be last. This is the reverse order
         * compared with ReflectionUtils.fieldOf, which is why we need to reverse here. */
        Collections.reverse(fields);
        return fields;
    }

    private List<IPDField> getIPDFields(Class<?> clazz) {
        List<IPDField> fields = new ArrayList<>();
        for (Field field : ReflectionUtils.fieldsOf(clazz)) {
            if (!Map.class.isAssignableFrom(field.getType())
                    && !(isScalaPresent() && scala.collection.Map.class.isAssignableFrom(field.getType()))) {
                for (PersonalData personalData : getPersonalDataAnn(field)) {
                    if (personalData.scope() != Scope.DEFAULT) {
                        throw ExceptionFactory.annScopeNotDefaultOnNonMapField(field);
                    }
                    ReflectionUtils.ensureAccessible(field);
                    if (!isTpeType(field.getGenericType())) {
                        throw ExceptionFactory.annWrongTypeForIPD(personalData.scope(), field, field.getGenericType());
                    }
                    fields.add(new IPDField(clazz,
                                            field,
                                            personalData.group(),
                                            personalData.replacement(),
                                            personalData.reencrypt()));
                }
            }
        }
        return fields;
    }

    private List<DPDField> getDPDFields(Class<?> clazz) {
        List<DPDField> fields = new ArrayList<>();
        for (Field field : ReflectionUtils.fieldsOf(clazz)) {
            if (!Map.class.isAssignableFrom(field.getType())
                    && !(isScalaPresent() && scala.collection.Map.class.isAssignableFrom(field.getType()))) {
                for (DeepPersonalData deepPersonalData : getDeepPersonalDataAnn(field)) {
                    if (deepPersonalData.scope() != Scope.DEFAULT) {
                        throw ExceptionFactory.annScopeNotDefaultOnNonMapField(field);
                    }
                    ReflectionUtils.ensureAccessible(field);
                    if (!isDpdType(field.getGenericType())) {
                        throw ExceptionFactory.annWrongTypeForDPD(deepPersonalData.scope(),
                                                                  field,
                                                                  field.getGenericType());
                    }
                    fields.add(new DPDField(field));
                }
            }
        }
        return fields;
    }

    private List<SPDField> getSPDFields(Class<?> clazz) {
        List<SPDField> fields = new ArrayList<>();
        Set<Field> storageFields = new HashSet<>();
        for (Field field : ReflectionUtils.fieldsOf(clazz)) {
            for (SerializedPersonalData serializedPersonalData : getSerializedPersonalDataAnn(field)) {
                ReflectionUtils.ensureAccessible(field);
                String tmpStorageFieldName = serializedPersonalData.storageField();
                if (tmpStorageFieldName.isEmpty()) {
                    tmpStorageFieldName = field.getName() + "Encrypted";
                }
                final String storageFieldName = tmpStorageFieldName;
                Field storageField = stream(ReflectionUtils.fieldsOf(clazz).spliterator(), false)
                        .filter(f -> f.getName().equals(storageFieldName))
                        .findFirst()
                        .orElseThrow(() -> ExceptionFactory.annStorageFieldMissing(storageFieldName, field));
                if (!(storageField.getType().equals(byte[].class) || storageField.getType().equals(String.class))) {
                    throw ExceptionFactory.annStorageFieldWrongType(storageField, field);
                }
                if (storageFields.contains(storageField)) {
                    throw ExceptionFactory.annStorageFieldUsedMultipleTimes(storageField);
                }
                fields.add(new SPDField(clazz,
                                        field,
                                        serializedPersonalData.group(),
                                        storageField,
                                        serializedPersonalData.replacement()));
                storageFields.add(storageField);
            }
        }
        return fields;
    }

    private List<MPDField> getMPDFields(Class<?> clazz) {
        List<MPDField> fields = new ArrayList<>();
        for (Field field : ReflectionUtils.fieldsOf(clazz)) {
            if (Map.class.isAssignableFrom(field.getType())
                    || (isScalaPresent() && scala.collection.Map.class.isAssignableFrom(field.getType()))) {
                List<DataSubjIdField> dataSubjIdFields = new ArrayList<>();
                Set<String> groups = new HashSet<>();
                List<MapKeyField> mapKeyFields = new ArrayList<>();
                List<MapValueField> mapValueFields = new ArrayList<>();
                for (DataSubjectId dataSubjectId : getDataSubjectIdAnn(field)) {
                    if (dataSubjectId.scope() != Scope.KEY) {
                        throw ExceptionFactory.annSubjectIdOnMapMustBeKey(field, dataSubjectId.scope());
                    }
                    if (groups.contains(dataSubjectId.group())) {
                        throw ExceptionFactory.annConflictingDataSubjectId(clazz, dataSubjectId.group());
                    }
                    groups.add(dataSubjectId.group());
                    dataSubjIdFields.add(new DataSubjIdField(field, dataSubjectId.group(), dataSubjectId.prefix()));
                }
                for (PersonalData personalData : getPersonalDataAnn(field)) {
                    IPDField ipdField = new IPDField(clazz,
                                                     field,
                                                     personalData.group(),
                                                     personalData.replacement(),
                                                     personalData.reencrypt());
                    switch (personalData.scope()) {
                        case DEFAULT:
                            throw ExceptionFactory.annScopeDefaultOnMapField(field);
                        case KEY:
                            mapKeyFields.add(ipdField);
                            break;
                        case VALUE:
                            mapValueFields.add(ipdField);
                            break;
                        case BOTH:
                            mapKeyFields.add(ipdField);
                            mapValueFields.add(ipdField);
                            break;
                    }
                }
                for (DeepPersonalData personalData : getDeepPersonalDataAnn(field)) {
                    DPDField dpdField = new DPDField(field);
                    switch (personalData.scope()) {
                        case DEFAULT:
                            throw ExceptionFactory.annScopeDefaultOnMapField(field);
                        case KEY:
                            mapKeyFields.add(dpdField);
                            break;
                        case VALUE:
                            mapValueFields.add(dpdField);
                            break;
                        case BOTH:
                            mapKeyFields.add(dpdField);
                            mapValueFields.add(dpdField);
                            break;
                    }
                }
                Map<String, List<MapKeyField>> mapKeyIPDFieldsPerGroup =
                        mapKeyFields.stream().filter(x -> x instanceof IPDField)
                                    .collect(Collectors.groupingBy(x -> ((IPDField) x).getGroup()));
                for (String group : mapKeyIPDFieldsPerGroup.keySet()) {
                    if (mapKeyIPDFieldsPerGroup.get(group).size() > 1) {
                        throw ExceptionFactory.annMapOverlappingScope(field, Scope.KEY, group);
                    }
                }
                if (mapKeyFields.size() > 0 && dataSubjIdFields.size() > 0) {
                    throw ExceptionFactory.annMapOverlappingScope(field, Scope.KEY);
                }
                for (MapKeyField mapKeyField : mapKeyFields) {
                    if (mapKeyField instanceof IPDField && !isTpeType(getMapKeyType(field.getGenericType()))) {
                        throw ExceptionFactory.annWrongTypeForIPD(Scope.KEY,
                                                                  field,
                                                                  getMapKeyType(field.getGenericType()));
                    }
                    if (mapKeyField instanceof DPDField && !isDpdType(getMapKeyType(field.getGenericType()))) {
                        throw ExceptionFactory.annWrongTypeForDPD(Scope.KEY,
                                                                  field,
                                                                  getMapKeyType(field.getGenericType()));
                    }
                }
                Map<String, List<MapValueField>> mapValueIPDFieldsPerGroup =
                        mapValueFields.stream().filter(x -> x instanceof IPDField)
                                      .collect(Collectors.groupingBy(x -> ((IPDField) x).getGroup()));
                for (String group : mapValueIPDFieldsPerGroup.keySet()) {
                    if (mapValueIPDFieldsPerGroup.get(group).size() > 1) {
                        throw ExceptionFactory.annMapOverlappingScope(field, Scope.VALUE, group);
                    }
                }
                for (MapValueField mapValueField : mapValueFields) {
                    if (mapValueField instanceof IPDField && !isTpeType(getMapValueType(field.getGenericType()))) {
                        throw ExceptionFactory.annWrongTypeForIPD(Scope.VALUE,
                                                                  field,
                                                                  getMapValueType(field.getGenericType()));
                    }
                    if (mapValueField instanceof DPDField && !isDpdType(getMapValueType(field.getGenericType()))) {
                        throw ExceptionFactory.annWrongTypeForDPD(Scope.VALUE,
                                                                  field,
                                                                  getMapValueType(field.getGenericType()));
                    }
                }
                fields.add(new MPDField(field, dataSubjIdFields, mapKeyFields, mapValueFields));
            }
        }
        return fields;
    }

    /*******************************************************************************************
     *
     * Utility methods to get all annotations of a certain type on a field, taking into account
     * that we may need to get them from a container annotation.
     *
     *******************************************************************************************/

    private List<DataSubjectId> getDataSubjectIdAnn(Field field) {
        List<DataSubjectId> list = new ArrayList<>();
        DataSubjectIdContainer container = AnnotationUtils.findAnnotation(field, DataSubjectIdContainer.class);
        if (container != null) {
            for (DataSubjectId personalData : container.value()) {
                list.add(personalData);
            }
        }
        DataSubjectId annotation = AnnotationUtils.findAnnotation(field, DataSubjectId.class);
        if (annotation != null) {
            list.add(annotation);
        }
        return list;
    }

    private List<PersonalData> getPersonalDataAnn(Field field) {
        List<PersonalData> list = new ArrayList<>();
        PersonalDataContainer container = AnnotationUtils.findAnnotation(field, PersonalDataContainer.class);
        if (container != null) {
            for (PersonalData personalData : container.value()) {
                list.add(personalData);
            }
        }
        PersonalData annotation = AnnotationUtils.findAnnotation(field, PersonalData.class);
        if (annotation != null) {
            list.add(annotation);
        }
        /* Reversing so we process them deepest-first. */
        Collections.reverse(list);
        return list;
    }

    private List<DeepPersonalData> getDeepPersonalDataAnn(Field field) {
        List<DeepPersonalData> list = new ArrayList<>();
        DeepPersonalDataContainer container = AnnotationUtils.findAnnotation(field, DeepPersonalDataContainer.class);
        if (container != null) {
            for (DeepPersonalData personalData : container.value()) {
                list.add(personalData);
            }
        }
        DeepPersonalData annotation = AnnotationUtils.findAnnotation(field, DeepPersonalData.class);
        if (annotation != null) {
            list.add(annotation);
        }
        return list;
    }

    private List<SerializedPersonalData> getSerializedPersonalDataAnn(Field field) {
        List<SerializedPersonalData> list = new ArrayList<>();
        SerializedPersonalData annotation = AnnotationUtils.findAnnotation(field, SerializedPersonalData.class);
        if (annotation != null) {
            list.add(annotation);
        }
        return list;
    }

    /*******************************************************************************************
     *
     * Utility methods for testing type properties. TPE types are String,byte[] and Collections
     * and arrays of TPE types. DPD types are classes annotated with 1 or more ADPM annotations,
     * and Collections and arrays of DPD types. The recursion over type structure is implemented
     * once for both cases.
     *
     *******************************************************************************************/

    private Type getMapKeyType(Type type) {
        return getMapType(type, 0);
    }

    private Type getMapValueType(Type type) {
        return getMapType(type, 1);
    }

    private Type getMapType(Type type, int index) {
        Type mapType = GenericTypeReflector.getExactSuperType(type, Map.class);
        if (mapType == null && isScalaPresent()) {
            mapType = GenericTypeReflector.getExactSuperType(type, scala.collection.Map.class);
        }
        if (mapType instanceof Class<?>) {
            return Object.class;
        } else if (mapType instanceof ParameterizedType) {
            return ((ParameterizedType) mapType).getActualTypeArguments()[index];
        } else {
            throw ExceptionFactory.unexpectedTypeInModelInspector(mapType);
        }
    }

    private boolean isTpeType(Type type) {
        return isType(type, simpleTpeTest);
    }

    private boolean isDpdType(Type type) {
        return isType(type, simpleDpdTest);
    }

    private final Function<Type, Boolean> simpleTpeTest = type -> {
        if (String.class.equals(type)) {
            return true;
        } else if (type instanceof Class<?>
                && ((Class<?>) type).isArray()
                && byte.class.equals(GenericTypeReflector.getArrayComponentType(type))) {
            return true;
        } else {
            return false;
        }
    };

    private final Function<Type, Boolean> simpleDpdTest = type -> {
        if (type instanceof Class<?>) {
            if (AnnotationUtils.findAnnotationAttributes((Class<?>) type, PersonalDataType.class).isPresent()) {
                return true;
            }
            for (Field field : ReflectionUtils.fieldsOf((Class<?>) type)) {
                if (AnnotationUtils.findAnnotation(field, DataSubjectId.class) != null) {
                    return true;
                }
                if (AnnotationUtils.findAnnotation(field, PersonalData.class) != null) {
                    return true;
                }
                if (AnnotationUtils.findAnnotation(field, PersonalDataContainer.class) != null) {
                    return true;
                }
                if (AnnotationUtils.findAnnotation(field, DeepPersonalData.class) != null) {
                    return true;
                }
                if (AnnotationUtils.findAnnotation(field, DeepPersonalDataContainer.class) != null) {
                    return true;
                }
                if (AnnotationUtils.findAnnotation(field, SerializedPersonalData.class) != null) {
                    return true;
                }
            }
            return false;
        } else {
            return false;
        }
    };

    private boolean isType(Type type, Function<Type, Boolean> test) {
        if (test.apply(type)) {
            return true;
        } else if (type instanceof Class<?>) {
            return isType((Class<?>) type, test);
        } else if (type instanceof GenericArrayType) {
            return isType((GenericArrayType) type, test);
        } else if (type instanceof ParameterizedType) {
            return isType((ParameterizedType) type, test);
        } else if (type instanceof WildcardType) {
            return isType((WildcardType) type, test);
        } else {
            return false;
        }
    }

    private boolean isType(Class<?> type, Function<Type, Boolean> test) {
        if (type.isArray() && isType(GenericTypeReflector.getArrayComponentType(type), test)) {
            return true;
        } else {
            return false;
        }
    }

    private boolean isType(GenericArrayType type, Function<Type, Boolean> test) {
        return isType(type.getGenericComponentType(), test);
    }

    private boolean isType(ParameterizedType type, Function<Type, Boolean> test) {
        return isTypeAsTemplate(type, test, Collection.class) || (isScalaPresent() && (
                isTypeAsTemplate(type, test, scala.collection.Iterable.class)
                        || isTypeAsTemplate(type, test, scala.Option.class))
        );
    }

    private boolean isTypeAsTemplate(ParameterizedType type, Function<Type, Boolean> test, Class<?> template) {
        Type collectionType = GenericTypeReflector.getExactSuperType(type, template);
        if (collectionType instanceof Class<?>) {
            return isType((Type) Object.class, test);
        } else if (collectionType instanceof ParameterizedType) {
            return isType(((ParameterizedType) collectionType).getActualTypeArguments()[0], test);
        } else {
            return false;
        }
    }

    private boolean isType(WildcardType type, Function<Type, Boolean> test) {
        for (Type upperBound : type.getUpperBounds()) {
            if (isType(upperBound, test)) {
                return true;
            }
        }
        return false;
    }
}
