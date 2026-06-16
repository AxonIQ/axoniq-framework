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

package io.axoniq.framework.messaging.transformation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Branch coverage for {@link StructuralPayloadTypes#isStructural(Class)}. Each structural
 * category (arrays, maps, collections, char sequences, numbers, Jackson tree nodes) has at
 * least one positive case, and a plain domain POJO covers the negative case.
 */
final class StructuralPayloadTypesTest {

    @Nested
    final class Arrays {

        @Test
        void byteArrayIsStructural() {
            assertThat(StructuralPayloadTypes.isStructural(byte[].class)).isTrue();
        }

        @Test
        void primitiveIntArrayIsStructural() {
            assertThat(StructuralPayloadTypes.isStructural(int[].class)).isTrue();
        }

        @Test
        void objectArrayIsStructural() {
            assertThat(StructuralPayloadTypes.isStructural(String[].class)).isTrue();
        }
    }

    @Nested
    final class Maps {

        @Test
        void mapInterfaceIsStructural() {
            assertThat(StructuralPayloadTypes.isStructural(Map.class)).isTrue();
        }

        @Test
        void hashMapImplementationIsStructural() {
            assertThat(StructuralPayloadTypes.isStructural(HashMap.class)).isTrue();
        }

        @Test
        void treeMapImplementationIsStructural() {
            assertThat(StructuralPayloadTypes.isStructural(TreeMap.class)).isTrue();
        }
    }

    @Nested
    final class Collections {

        @Test
        void collectionInterfaceIsStructural() {
            assertThat(StructuralPayloadTypes.isStructural(Collection.class)).isTrue();
        }

        @Test
        void listInterfaceIsStructural() {
            assertThat(StructuralPayloadTypes.isStructural(List.class)).isTrue();
        }

        @Test
        void arrayListImplementationIsStructural() {
            assertThat(StructuralPayloadTypes.isStructural(ArrayList.class)).isTrue();
        }

        @Test
        void linkedListImplementationIsStructural() {
            assertThat(StructuralPayloadTypes.isStructural(LinkedList.class)).isTrue();
        }
    }

    @Nested
    final class CharSequences {

        @Test
        void stringIsStructural() {
            assertThat(StructuralPayloadTypes.isStructural(String.class)).isTrue();
        }

        @Test
        void stringBuilderIsStructural() {
            assertThat(StructuralPayloadTypes.isStructural(StringBuilder.class)).isTrue();
        }
    }

    @Nested
    final class Numbers {

        @Test
        void integerIsStructural() {
            assertThat(StructuralPayloadTypes.isStructural(Integer.class)).isTrue();
        }

        @Test
        void doubleIsStructural() {
            assertThat(StructuralPayloadTypes.isStructural(Double.class)).isTrue();
        }

        @Test
        void longIsStructural() {
            assertThat(StructuralPayloadTypes.isStructural(Long.class)).isTrue();
        }
    }

    @Nested
    final class JacksonTreeNodes {

        @Test
        void jackson2JsonNodeBaseClassIsStructural() {
            // Jackson 2's base tree-node class itself matches the FQN walk on its first hop.
            assertThat(StructuralPayloadTypes.isStructural(JsonNode.class)).isTrue();
        }

        @Test
        void jackson2ObjectNodeSubclassIsStructural() {
            // Concrete subclass walks up the hierarchy until it hits the Jackson 2 JsonNode
            // FQN, exercising the multi-hop superclass walk.
            ObjectNode node = JsonNodeFactory.instance.objectNode();
            assertThat(StructuralPayloadTypes.isStructural(node.getClass())).isTrue();
        }

        @Test
        void jackson3JsonNodeBaseClassIsStructural() {
            // Covers the Jackson 3 FQN branch of the OR check independently from Jackson 2.
            assertThat(StructuralPayloadTypes.isStructural(tools.jackson.databind.JsonNode.class)).isTrue();
        }

        @Test
        void jackson3ObjectNodeSubclassIsStructural() {
            // Concrete Jackson 3 subclass exercises the superclass walk hitting the
            // Jackson 3 FQN, mirroring the Jackson 2 multi-hop case.
            tools.jackson.databind.node.ObjectNode node =
                    tools.jackson.databind.node.JsonNodeFactory.instance.objectNode();
            assertThat(StructuralPayloadTypes.isStructural(node.getClass())).isTrue();
        }

        @Test
        void unrelatedClassWithJsonNodeSuffixIsNotStructural() {
            // Guards against accidental matching by simple-name suffix. The implementation
            // must match the fully-qualified name, not the simple name.
            assertThat(StructuralPayloadTypes.isStructural(LookAlikeJsonNode.class)).isFalse();
        }
    }

    @Nested
    final class NonStructural {

        @Test
        void plainPojoIsNotStructural() {
            assertThat(StructuralPayloadTypes.isStructural(DomainPojo.class)).isFalse();
        }

        @Test
        void enumIsNotStructural() {
            assertThat(StructuralPayloadTypes.isStructural(SampleEnum.class)).isFalse();
        }

        @Test
        void objectClassIsNotStructural() {
            // The hierarchy walk must terminate at Object.class without matching: covers
            // the `current == Object.class` exit branch.
            assertThat(StructuralPayloadTypes.isStructural(Object.class)).isFalse();
        }

        @Test
        void unrelatedInterfaceIsNotStructural() {
            // An interface that is neither Map / Collection / CharSequence / Number reaches
            // the Jackson walk. Interface classes have `null` as superclass, which exits the
            // loop via the `current != null` check (distinct from the Object.class exit).
            assertThat(StructuralPayloadTypes.isStructural(Runnable.class)).isFalse();
        }
    }

    /** Plain domain payload: not a carrier, not a tree node. */
    private static final class DomainPojo {
    }

    /** Negative-case helper: simple name ends in {@code JsonNode} but the FQN does not match. */
    private static final class LookAlikeJsonNode {
    }

    /** Negative-case helper proving plain enums are not classified as structural. */
    private enum SampleEnum {
        ONE
    }
}
