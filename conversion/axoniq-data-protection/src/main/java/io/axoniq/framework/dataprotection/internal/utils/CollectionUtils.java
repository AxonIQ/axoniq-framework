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

import io.axoniq.framework.dataprotection.api.FieldEncrypter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Queue;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.LinkedTransferQueue;
import java.util.function.Function;

/**
 * Utility class for working with Java Collections. Provides methods to modify
 * immutable and wrapped collections through reflection.
 *
 * @author Frans van Buul
 */
public abstract class CollectionUtils {

    /* Intentionally choosing a different class for naming the logger. */
    private static final Logger log = LoggerFactory.getLogger(FieldEncrypter.class);

    private CollectionUtils() {
        throw new Error("non-instantiable utils class");
    }

    private static class WrapperType {
        private final Object test;
        private final Function<Object, Object> wrap;
        private Class<?> clazz;
        private Field field;
        private Field keysField;
        private Field valuesField;
        private Field entriesField;
        private boolean collection;
        private boolean singleton;
        private boolean array;
        private boolean map;

        WrapperType(Object test, Function<Object, Object> wrap) {
            this.test = test;
            this.wrap = wrap;
        }
    }

    private static class SingletonMapWrapperType {
        private final Map.Entry test;
        private final Function<Map.Entry, Object> wrap;
        private Class<?> clazz;
        private Field keyField;
        private Field valueField;
        private Field keysField;
        private Field valuesField;
        private Field entriesField;

        SingletonMapWrapperType(Map.Entry test, Function<Map.Entry, Object> wrap) {
            this.test = test;
            this.wrap = wrap;
        }
    }

    private static List<WrapperType> wrapperTypes = new ArrayList<>();
    private static Map<Class<?>, WrapperType> wrapperTypeMap = new HashMap<>();
    private static SingletonMapWrapperType singletonMapWrapperType;

    static {
        wrapperTypes.add(new WrapperType(new Object[1], x -> Arrays.asList((Object[])x)));
        wrapperTypes.add(new WrapperType(new LinkedList(), x -> Collections.asLifoQueue((Deque)x)));
        wrapperTypes.add(new WrapperType(new HashSet(), x -> Collections.checkedCollection((Collection<Object>)x, Object.class)));
        wrapperTypes.add(new WrapperType(new ArrayList(), x -> Collections.checkedList((List<Object>)x, Object.class)));
        wrapperTypes.add(new WrapperType(new LinkedList(), x -> Collections.checkedList((List<Object>)x, Object.class)));
        wrapperTypes.add(new WrapperType(new HashMap(Collections.singletonMap(Long.valueOf(1L), Long.valueOf(1L))), x -> Collections.checkedMap((Map<Object, Object>)x, Object.class, Object.class)));
        wrapperTypes.add(new WrapperType(new TreeMap(Collections.singletonMap(Long.valueOf(1L), Long.valueOf(1L))), x -> Collections.checkedMap((Map<Object, Object>)x, Object.class, Object.class)));
        wrapperTypes.add(new WrapperType(new LinkedHashMap(Collections.singletonMap(Long.valueOf(1L), Long.valueOf(1L))), x -> Collections.checkedMap((Map<Object, Object>)x, Object.class, Object.class)));
        wrapperTypes.add(new WrapperType(new ConcurrentSkipListMap(Collections.singletonMap(Long.valueOf(1L), Long.valueOf(1L))), x -> Collections.checkedMap((Map<Object, Object>)x, Object.class, Object.class)));
        wrapperTypes.add(new WrapperType(new TreeMap(Collections.singletonMap(Long.valueOf(1L), Long.valueOf(1L))), x -> Collections.checkedSortedMap((SortedMap<Object, Object>)x, Object.class, Object.class)));
        wrapperTypes.add(new WrapperType(new ConcurrentSkipListMap(Collections.singletonMap(Long.valueOf(1L), Long.valueOf(1L))), x -> Collections.checkedSortedMap((SortedMap<Object, Object>)x, Object.class, Object.class)));
        wrapperTypes.add(new WrapperType(new TreeSet(), x -> Collections.checkedNavigableSet((NavigableSet<Object>)x, Object.class)));
        wrapperTypes.add(new WrapperType(new LinkedList(), x -> Collections.checkedQueue((Queue<Object>)x, Object.class)));
        wrapperTypes.add(new WrapperType(new HashSet(), x -> Collections.checkedSet((Set<Object>)x, Object.class)));
        wrapperTypes.add(new WrapperType(new TreeSet(), x -> Collections.checkedSortedSet((SortedSet<Object>)x, Object.class)));
        wrapperTypes.add(new WrapperType(Long.valueOf(1L), x -> Collections.singleton(x)));
        wrapperTypes.add(new WrapperType(Long.valueOf(1L), x -> Collections.singletonList(x)));
        wrapperTypes.add(new WrapperType(new LinkedList(), x -> Collections.synchronizedCollection((Collection)x)));
        wrapperTypes.add(new WrapperType(new LinkedList(), x -> Collections.synchronizedList((List)x)));
        wrapperTypes.add(new WrapperType(new ArrayList(), x -> Collections.synchronizedCollection((Collection)x)));
        wrapperTypes.add(new WrapperType(new ArrayList(), x -> Collections.synchronizedList((List)x)));
        wrapperTypes.add(new WrapperType(new TreeSet(), x -> Collections.synchronizedNavigableSet((NavigableSet)x)));
        wrapperTypes.add(new WrapperType(new TreeSet(), x -> Collections.synchronizedSet((Set)x)));
        wrapperTypes.add(new WrapperType(new HashSet(), x -> Collections.synchronizedSet((Set)x)));
        wrapperTypes.add(new WrapperType(new TreeSet(), x -> Collections.synchronizedSortedSet((SortedSet)x)));
        wrapperTypes.add(new WrapperType(new HashMap(Collections.singletonMap(Long.valueOf(1L), Long.valueOf(1L))), x -> Collections.synchronizedMap((Map<Object, Object>)x)));
        wrapperTypes.add(new WrapperType(new TreeMap(Collections.singletonMap(Long.valueOf(1L), Long.valueOf(1L))), x -> Collections.synchronizedMap((Map<Object, Object>)x)));
        wrapperTypes.add(new WrapperType(new LinkedHashMap(Collections.singletonMap(Long.valueOf(1L), Long.valueOf(1L))), x -> Collections.synchronizedMap((Map<Object, Object>)x)));
        wrapperTypes.add(new WrapperType(new ConcurrentSkipListMap(Collections.singletonMap(Long.valueOf(1L), Long.valueOf(1L))), x -> Collections.synchronizedMap((Map<Object, Object>)x)));
        wrapperTypes.add(new WrapperType(new TreeMap(Collections.singletonMap(Long.valueOf(1L), Long.valueOf(1L))), x -> Collections.synchronizedSortedMap((SortedMap<Object, Object>)x)));
        wrapperTypes.add(new WrapperType(new ConcurrentSkipListMap(Collections.singletonMap(Long.valueOf(1L), Long.valueOf(1L))), x -> Collections.synchronizedSortedMap((SortedMap<Object, Object>)x)));
        wrapperTypes.add(new WrapperType(new HashSet(), x -> Collections.unmodifiableCollection((Collection)x)));
        wrapperTypes.add(new WrapperType(new ArrayList(), x -> Collections.unmodifiableList((List)x)));
        wrapperTypes.add(new WrapperType(new LinkedList(), x -> Collections.unmodifiableList((List)x)));
        wrapperTypes.add(new WrapperType(new TreeSet(), x -> Collections.unmodifiableNavigableSet((NavigableSet)x)));
        wrapperTypes.add(new WrapperType(new TreeSet(), x -> Collections.unmodifiableSet((Set)x)));
        wrapperTypes.add(new WrapperType(new HashSet(), x -> Collections.unmodifiableSet((Set)x)));
        wrapperTypes.add(new WrapperType(new TreeSet(), x -> Collections.unmodifiableSortedSet((SortedSet)x)));
        wrapperTypes.add(new WrapperType(new HashMap(Collections.singletonMap(Long.valueOf(1L), Long.valueOf(1L))), x -> Collections.unmodifiableMap((Map<Object, Object>)x)));
        wrapperTypes.add(new WrapperType(new TreeMap(Collections.singletonMap(Long.valueOf(1L), Long.valueOf(1L))), x -> Collections.unmodifiableMap((Map<Object, Object>)x)));
        wrapperTypes.add(new WrapperType(new LinkedHashMap(Collections.singletonMap(Long.valueOf(1L), Long.valueOf(1L))), x -> Collections.unmodifiableMap((Map<Object, Object>)x)));
        wrapperTypes.add(new WrapperType(new ConcurrentSkipListMap(Collections.singletonMap(Long.valueOf(1L), Long.valueOf(1L))), x -> Collections.unmodifiableMap((Map<Object, Object>)x)));
        wrapperTypes.add(new WrapperType(new TreeMap(Collections.singletonMap(Long.valueOf(1L), Long.valueOf(1L))), x -> Collections.unmodifiableSortedMap((SortedMap<Object, Object>)x)));
        wrapperTypes.add(new WrapperType(new ConcurrentSkipListMap(Collections.singletonMap(Long.valueOf(1L), Long.valueOf(1L))), x -> Collections.unmodifiableSortedMap((SortedMap<Object, Object>)x)));
        singletonMapWrapperType = new SingletonMapWrapperType(
                Collections.singletonMap(new Object(), new Object()).entrySet().iterator().next(),
                x -> Collections.singletonMap(((Map.Entry)x).getKey(), ((Map.Entry)x).getValue()));

        for(WrapperType wrapperType : wrapperTypes) {
            Object wrapped = wrapperType.wrap.apply(wrapperType.test);
            wrapperType.clazz = wrapped.getClass();
            for(Field field : ReflectionUtils.fieldsOf(wrapperType.clazz)) {
                ReflectionUtils.ensureAccessible(field);
                Object fieldValue = ReflectionUtils.getFieldValue(field, wrapped);
                if(fieldValue == wrapperType.test) {
                    wrapperType.field = field;
                    break;
                }
            }
            if(wrapperType.field == null) {
                log.error(ExceptionFactory.unableToAnalyzeWrapper(wrapperType.clazz));
            } else {
                if (wrapperType.test instanceof Collection) {
                    wrapperType.collection = true;
                } else if (wrapperType.test.getClass().isArray()) {
                    wrapperType.array = true;
                } else if (Map.class.isAssignableFrom(wrapperType.test.getClass())) {
                    wrapperType.map = true;
                    for(Field field : ReflectionUtils.fieldsOf(wrapperType.clazz)) {
                        Object fieldValue = ReflectionUtils.getFieldValue(field, wrapped);
                        if(fieldValue instanceof Collection && Modifier.isTransient(field.getModifiers())) {
                            Collection collection = ((Collection)fieldValue);
                            if(collection.size() == 1) {
                                Object element = collection.iterator().next();
                                if(element == ((Map)wrapperType.test).keySet().iterator().next()) {
                                    wrapperType.keysField = field;
                                } else if(element == ((Map)wrapperType.test).values().iterator().next()) {
                                    wrapperType.valuesField = field;
                                } else if(element instanceof Map.Entry) {
                                    wrapperType.entriesField = field;
                                }
                            }
                        }
                    }
                } else {
                    wrapperType.singleton = true;
                }
                wrapperTypeMap.put(wrapperType.clazz, wrapperType);
            }
        }

        {
            SingletonMapWrapperType wrapperType = singletonMapWrapperType;
            Map wrapped = (Map)wrapperType.wrap.apply(wrapperType.test);
            wrapperType.clazz = wrapped.getClass();
            /* Enforce lazy init */
            Assert.isTrue(wrapped.keySet().iterator().next() == wrapperType.test.getKey(), () -> "Assertion failed");
            Assert.isTrue(wrapped.values().iterator().next() == wrapperType.test.getValue(), () -> "Assertion failed");
            Assert.isTrue(((Map.Entry)wrapped.entrySet().iterator().next()).getKey() == wrapperType.test.getKey(), () -> "Assertion failed");
            for(Field field : ReflectionUtils.fieldsOf(wrapperType.clazz)) {
                ReflectionUtils.ensureAccessible(field);
                Object fieldValue = ReflectionUtils.getFieldValue(field, wrapped);
                if(fieldValue == wrapperType.test.getKey()) {
                    wrapperType.keyField = field;
                } else if(fieldValue == wrapperType.test.getValue()) {
                    wrapperType.valueField = field;
                } else if(fieldValue instanceof Collection && Modifier.isTransient(field.getModifiers())) {
                    Collection collection = ((Collection)fieldValue);
                    if(collection.size() == 1) {
                        Object element = collection.iterator().next();
                        if(element == wrapperType.test.getKey()) {
                            wrapperType.keysField = field;
                        } else if(element == wrapperType.test.getValue()) {
                            wrapperType.valuesField = field;
                        } else if(element instanceof Map.Entry) {
                            wrapperType.entriesField = field;
                        }
                    }
                }
            }
            if(wrapperType.keyField == null || wrapperType.valueField == null) {
                log.error(ExceptionFactory.unableToAnalyzeWrapper(wrapperType.clazz));
                singletonMapWrapperType = null;
            }
        }

    }

    public static void replaceAll(Collection collection, List newElements) {
        WrapperType wrapperType = wrapperTypeMap.get(collection.getClass());
        if(wrapperType == null) {
            collection.clear();
            collection.addAll(newElements);
        } else if(wrapperType.collection) {
            replaceAll(ReflectionUtils.getFieldValue(wrapperType.field, collection), newElements);
        } else if(wrapperType.array) {
            Object[] array = ReflectionUtils.getFieldValue(wrapperType.field, collection);
            if(array.length != newElements.size()) {
                throw ExceptionFactory.unexpectedSizeChange();
            }
            for(int i = 0; i < array.length; i++) {
                array[i] = newElements.get(i);
            }
        } else if(wrapperType.singleton) {
            if(newElements.size() != 1) {
                throw ExceptionFactory.unexpectedSizeChange();
            }
            ReflectionUtils.setFieldValue(wrapperType.field, collection, newElements.get(0));
        } else {
            throw ExceptionFactory.collectionUtilsMisconfig(wrapperType.clazz);
        }
    }

    public static void replaceAll(Map map, Map newElements) {
        if(singletonMapWrapperType == null
                || !singletonMapWrapperType.clazz.equals(map.getClass())) {
            WrapperType wrapperType = wrapperTypeMap.get(map.getClass());
            if (wrapperType == null) {
                map.clear();
                map.putAll(newElements);
            } else if (wrapperType.map) {
                replaceAll(ReflectionUtils.getFieldValue(wrapperType.field, map), newElements);
                if(wrapperType.keysField != null) {
                    ReflectionUtils.setFieldValue(wrapperType.keysField, map, null);
                }
                if(wrapperType.valuesField != null) {
                    ReflectionUtils.setFieldValue(wrapperType.valuesField, map, null);
                }
                if(wrapperType.entriesField != null) {
                    ReflectionUtils.setFieldValue(wrapperType.entriesField, map, null);
                }
            } else {
                throw ExceptionFactory.collectionUtilsMisconfig(wrapperType.clazz);
            }
        } else {
            if(newElements.size() != 1) {
                ExceptionFactory.unexpectedSizeChange();
            }
            SingletonMapWrapperType wrapperType = singletonMapWrapperType;
            Object key = newElements.keySet().iterator().next();
            Object value = newElements.values().iterator().next();
            ReflectionUtils.setFieldValue(wrapperType.keyField, map, key);
            ReflectionUtils.setFieldValue(wrapperType.valueField, map, value);
            if(wrapperType.keysField != null) {
                ReflectionUtils.setFieldValue(wrapperType.keysField, map, null);
            }
            if(wrapperType.valuesField != null) {
                ReflectionUtils.setFieldValue(wrapperType.valuesField, map, null);
            }
            if(wrapperType.entriesField != null) {
                ReflectionUtils.setFieldValue(wrapperType.entriesField, map, null);
            }
        }
    }

    /**
     * Wrap the given {@code values} in a collection that matches the given {@code expectedType}. If the
     * {@code expectedType} is not compatible with any of the supported container type, this methods throws an {@link
     * UnsupportedOperationException}.
     *
     * @param expectedType The type that the returned collection should be compatible to
     * @param values       The values the returned collection must contain
     *
     * @return a Collection implementation that contains given {@code values} assignable to {@code expectedType}
     * @throws UnsupportedOperationException when the expected type is not supported
     */
    public static Collection buildContainer(Class<?> expectedType, List<Object> values) {
        if (expectedType.isInstance(values)) {
            return values;
        } else if (expectedType.isAssignableFrom(HashSet.class)) {
            return new HashSet(values);
        } else if (expectedType.isAssignableFrom(TreeSet.class)) {
            return new TreeSet(values);
        } else if (expectedType.isAssignableFrom(ArrayDeque.class)) {
            return new ArrayDeque(values);
        } else if (expectedType.isAssignableFrom(LinkedList.class)) {
            return new LinkedList(values);
        } else if (expectedType.isAssignableFrom(LinkedBlockingQueue.class)) {
            return new LinkedBlockingQueue(values);
        } else if (expectedType.isAssignableFrom(LinkedBlockingDeque.class)) {
            return new LinkedBlockingDeque(values);
        } else if (expectedType.isAssignableFrom(LinkedTransferQueue.class)) {
            return new LinkedTransferQueue(values);
        }
        throw new UnsupportedOperationException("Unsupported collection type: " + expectedType.getName());
    }

}
