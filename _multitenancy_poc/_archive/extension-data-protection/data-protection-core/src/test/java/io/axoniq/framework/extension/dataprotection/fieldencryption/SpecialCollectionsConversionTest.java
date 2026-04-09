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
package io.axoniq.framework.extension.dataprotection.fieldencryption;

import io.axoniq.framework.extension.dataprotection.api.DataSubjectId;
import io.axoniq.framework.extension.dataprotection.api.FieldEncrypter;
import io.axoniq.framework.extension.dataprotection.api.PersonalData;
import io.axoniq.framework.extension.dataprotection.cryptoengine.CryptoEngine;
import io.axoniq.framework.extension.dataprotection.cryptoengine.InMemoryCryptoEngine;
import io.axoniq.framework.extension.dataprotection.utils.TestUtils;
import org.axonframework.conversion.Converter;
import org.fluttercode.datafactory.impl.DataFactory;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;
import java.util.SortedSet;
import java.util.Spliterator;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static io.axoniq.framework.extension.dataprotection.utils.TestUtils.defaultTestConverter;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests encryption in special collections (immutable, singletons, etc.) using AF5 Converter API.
 * This is the AF5 equivalent of SpecialCollectionsTest which used the AF4 Serializer API.
 */
class SpecialCollectionsConversionTest {

    private FieldEncrypter fieldEncrypter;
    private Converter converter;

    @BeforeEach
    public void setUp() throws Exception {
        CryptoEngine cryptoEngine = new InMemoryCryptoEngine();
        cryptoEngine.registerEntitlementManager(TestUtils.mockEntitlementManager());
        converter = defaultTestConverter();
        fieldEncrypter = new FieldEncrypter(cryptoEngine, converter);
        DataFactory dataFactory = new DataFactory();
        dataFactory.randomize(ThreadLocalRandom.current().nextInt());
    }

    @Test
    public void nullMustStayNull() {
        A a = new A(1L, null);
        a = fieldEncrypter.encryptAndReturn(a);
        assertNull(a.data);
        a = fieldEncrypter.decryptAndReturn(a);
        assertNull(a.data);
    }

    @Test
    public void emptyList() {
        A a = new A(1L, Collections.EMPTY_LIST);
        a = fieldEncrypter.encryptAndReturn(a);
        assertTrue(a.data.isEmpty());
        a = fieldEncrypter.decryptAndReturn(a);
        assertTrue(a.data.isEmpty());
    }

    @Test
    public void singletonList() {
        A a = new A(1L, Collections.singletonList("TEST"));
        a = fieldEncrypter.encryptAndReturn(a);
        assertNotEquals("TEST", a.data.getFirst());
        a = fieldEncrypter.decryptAndReturn(a);
        assertEquals("TEST", a.data.getFirst());
    }

    @Test
    public void singletonSet() {
        B a = new B(1L, Collections.singleton("TEST"));
        a = fieldEncrypter.encryptAndReturn(a);
        assertFalse(a.data.contains("TEST"));
        a = fieldEncrypter.decryptAndReturn(a);
        assertTrue(a.data.contains("TEST"));
    }

    @Test
    public void unmodifiableList() {
        List<String> list = new ArrayList<>();
        list.add("TEST1");
        list.add("TEST2");
        A a = new A(1L, Collections.unmodifiableList(list));

        String converted1 = converter.convert(a, String.class);
        System.out.println(converted1);
        assertNotNull(converted1);
        assertTrue(converted1.contains("TEST"));

        a = fieldEncrypter.encryptAndReturn(a);

        String converted2 = converter.convert(a, String.class);
        System.out.println(converted2);
        assertNotNull(converted2);
        assertFalse(converted2.contains("TEST"));

        a = fieldEncrypter.decryptAndReturn(a);

        String converted3 = converter.convert(a, String.class);
        assertEquals(converted1, converted3);
    }

    @Test
    public void arrayAsList() {
        A a = new A(1L, Arrays.asList("TEST1", "TEST2"));

        String converted1 = converter.convert(a, String.class);
        System.out.println(converted1);
        assertNotNull(converted1);
        assertTrue(converted1.contains("TEST"));

        a = fieldEncrypter.encryptAndReturn(a);

        String converted2 = converter.convert(a, String.class);
        System.out.println(converted2);
        assertNotNull(converted2);
        assertFalse(converted2.contains("TEST"));

        a = fieldEncrypter.decryptAndReturn(a);

        String converted3 = converter.convert(a, String.class);
        assertEquals(converted1, converted3);
    }

    @Test
    public void synchronizedUnmodifiableList() {
        List<String> list = new ArrayList<>();
        list.add("TEST1");
        list.add("TEST2");
        A a = new A(1L, Collections.synchronizedList(Collections.unmodifiableList(list)));

        String converted1 = converter.convert(a, String.class);
        System.out.println(converted1);
        assertNotNull(converted1);
        assertTrue(converted1.contains("TEST"));

        a = fieldEncrypter.encryptAndReturn(a);

        String converted2 = converter.convert(a, String.class);
        System.out.println(converted2);
        assertNotNull(converted2);
        assertFalse(converted2.contains("TEST"));

        a = fieldEncrypter.decryptAndReturn(a);

        String converted3 = converter.convert(a, String.class);
        assertEquals(converted1, converted3);
    }

    @Test
    public void checkedUnmodifiableSortedSet() {
        SortedSet<String> set = new TreeSet<>();
        set.add("TEST1");
        set.add("TEST2");
        B a = new B(1L, Collections.checkedSortedSet(Collections.unmodifiableSortedSet(set), String.class));

        String converted1 = converter.convert(a, String.class);
        System.out.println(converted1);
        assertNotNull(converted1);
        assertTrue(converted1.contains("TEST"));

        a = fieldEncrypter.encryptAndReturn(a);

        String converted2 = converter.convert(a, String.class);
        System.out.println(converted2);
        assertNotNull(converted2);
        assertFalse(converted2.contains("TEST"));

        a = fieldEncrypter.decryptAndReturn(a);

        String converted3 = converter.convert(a, String.class);
        assertEquals(converted1, converted3);
    }

    @Test
    public void customImplementationList() {
        List<String> list = new CustomImmutableList("TEST1", "TEST2");
        A a = new A(1L, list);

        String converted1 = converter.convert(a, String.class);
        System.out.println(converted1);
        assertNotNull(converted1);
        assertTrue(converted1.contains("TEST"), "Original should contain TEST");

        a = fieldEncrypter.encryptAndReturn(a);

        String converted2 = converter.convert(a, String.class);
        System.out.println(converted2);
        assertNotNull(converted2);
        assertFalse(converted2.contains("TEST"), "Encrypted should NOT contain TEST");

        a = fieldEncrypter.decryptAndReturn(a);

        // Verify data values are preserved after encrypt/decrypt cycle
        assertEquals(1L, a.id());
        assertEquals(2, a.data().size());
        assertTrue(a.data().contains("TEST1"), "Should contain TEST1 after decrypt");
        assertTrue(a.data().contains("TEST2"), "Should contain TEST2 after decrypt");

        // NOTE: JacksonConverter (AF5) doesn't preserve custom List implementation types.
        // The type is normalized to ArrayList during conversion.
        // Unlike XStreamSerializer (AF4), this is expected behavior with the Converter API.
        // We verify the data is correct, but don't compare converted strings since the type changes.
    }

    public record A(@DataSubjectId long id, @PersonalData List<String> data) {

    }

    public record B(@DataSubjectId long id, @PersonalData Set<String> data) {

    }

    private static class CustomImmutableList implements List<String> {

        private final List<String> backend;

        public CustomImmutableList(String... values) {
            this.backend = Arrays.asList(values);
        }

        @Override
        public int size() {
            return backend.size();
        }

        @Override
        public boolean isEmpty() {
            return backend.isEmpty();
        }

        @Override
        public boolean contains(Object o) {
            return backend.contains(o);
        }

        @NotNull
        @Override
        public Iterator<String> iterator() {
            return backend.iterator();
        }

        @NotNull
        @Override
        public Object[] toArray() {
            return backend.toArray();
        }

        @NotNull
        @Override
        public <T> T[] toArray(@NotNull T[] a) {
            return backend.toArray(a);
        }

        @Override
        public boolean add(String s) {
            throw new UnsupportedOperationException("Immutable list");
        }

        @Override
        public boolean remove(Object o) {
            throw new UnsupportedOperationException("Immutable list");
        }

        @Override
        public boolean containsAll(@NotNull Collection<?> c) {
            return backend.containsAll(c);
        }

        @Override
        public boolean addAll(@NotNull Collection<? extends String> c) {
            throw new UnsupportedOperationException("Immutable list");
        }

        @Override
        public boolean addAll(int index, @NotNull Collection<? extends String> c) {
            throw new UnsupportedOperationException("Immutable list");
        }

        @Override
        public boolean removeAll(@NotNull Collection<?> c) {
            throw new UnsupportedOperationException("Immutable list");
        }

        @Override
        public boolean retainAll(@NotNull Collection<?> c) {
            throw new UnsupportedOperationException("Immutable list");
        }

        @Override
        public void clear() {
            throw new UnsupportedOperationException("Immutable list");
        }

        @Override
        public boolean equals(Object o) {
            return backend.equals(o);
        }

        @Override
        public int hashCode() {
            return backend.hashCode();
        }

        @Override
        public String get(int index) {
            return backend.get(index);
        }

        @Override
        public String set(int index, String element) {
            throw new UnsupportedOperationException("Immutable list");
        }

        @Override
        public void add(int index, String element) {
            throw new UnsupportedOperationException("Immutable list");
        }

        @Override
        public String remove(int index) {
            throw new UnsupportedOperationException("Immutable list");
        }

        @Override
        public int indexOf(Object o) {
            return backend.indexOf(o);
        }

        @Override
        public int lastIndexOf(Object o) {
            return backend.lastIndexOf(o);
        }

        @NotNull
        @Override
        public ListIterator<String> listIterator() {
            return backend.listIterator();
        }

        @NotNull
        @Override
        public ListIterator<String> listIterator(int index) {
            return backend.listIterator(index);
        }

        @NotNull
        @Override
        public List<String> subList(int fromIndex, int toIndex) {
            return backend.subList(fromIndex, toIndex);
        }

        @NotNull
        @Override
        public Spliterator<String> spliterator() {
            return backend.spliterator();
        }

        @Override
        public boolean removeIf(@NotNull Predicate<? super String> filter) {
            throw new UnsupportedOperationException("Immutable list");
        }

        @NotNull
        @Override
        public Stream<String> stream() {
            return backend.stream();
        }

        @NotNull
        @Override
        public Stream<String> parallelStream() {
            return backend.parallelStream();
        }

        @Override
        public void forEach(Consumer<? super String> action) {
            backend.forEach(action);
        }
    }
}
