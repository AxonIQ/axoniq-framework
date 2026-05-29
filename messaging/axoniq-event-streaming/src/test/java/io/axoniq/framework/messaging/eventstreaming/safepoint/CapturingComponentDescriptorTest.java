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

package io.axoniq.framework.messaging.eventstreaming.safepoint;

import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.DelegatingEventHandlingComponent;
import org.axonframework.messaging.eventhandling.EventHandlingComponent;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.replay.ReplayStatusChanged;
import org.axonframework.messaging.eventhandling.replay.ResetContext;
import org.junit.jupiter.api.Test;
import org.jspecify.annotations.NonNull;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class CapturingComponentDescriptorTest {

    @Test
    void capturesDirectEventHandlingComponent() {
        // given
        StubEventHandlingComponent inner = new StubEventHandlingComponent();

        // when
        List<EventHandlingComponent> captured =
                CapturingComponentDescriptor.capturing(EventHandlingComponent.class).capture(inner).toList();

        // then
        assertThat(captured).containsExactly(inner);
    }

    @Test
    void capturesThroughSingleDecorator() {
        // given - decorator wraps an inner component
        StubEventHandlingComponent inner = new StubEventHandlingComponent();
        TestDecorator outer = new TestDecorator(inner);

        // when
        List<EventHandlingComponent> captured =
                CapturingComponentDescriptor.capturing(EventHandlingComponent.class).capture(outer).toList();

        // then - both outer and inner are captured
        assertThat(captured).containsExactlyInAnyOrder(outer, inner);
    }

    @Test
    void capturesThroughMultipleDecorators() {
        // given - three layers of decorators
        StubEventHandlingComponent inner = new StubEventHandlingComponent();
        TestDecorator middle = new TestDecorator(inner);
        TestDecorator outer = new TestDecorator(middle);

        // when
        List<EventHandlingComponent> captured =
                CapturingComponentDescriptor.capturing(EventHandlingComponent.class).capture(outer).toList();

        // then - all three are captured
        assertThat(captured).containsExactlyInAnyOrder(outer, middle, inner);
    }

    @Test
    void dedupesByIdentityWhenSameComponentReachedFromMultiplePaths() {
        // given - both decorators wrap the same inner component
        StubEventHandlingComponent inner = new StubEventHandlingComponent();
        TestDecorator outerA = new TestDecorator(inner);
        TestTwoChildDecorator parent = new TestTwoChildDecorator(outerA, new TestDecorator(inner));

        // when
        List<EventHandlingComponent> captured =
                CapturingComponentDescriptor.capturing(EventHandlingComponent.class).capture(parent).toList();

        // then - inner is captured exactly once even though reached through two paths
        assertThat(captured).hasSize(4)
                            .contains(inner);
        assertThat(captured.stream().filter(c -> c == inner)).hasSize(1);
    }

    @Test
    void handlesCyclesWithoutInfiniteRecursion() {
        // given - components A and B reference each other
        CycleComponent a = new CycleComponent();
        CycleComponent b = new CycleComponent();
        a.peer = b;
        b.peer = a;

        // when
        List<EventHandlingComponent> captured =
                CapturingComponentDescriptor.capturing(EventHandlingComponent.class).capture(a).toList();

        // then - both components captured exactly once; no StackOverflowError
        assertThat(captured).hasSize(2)
                            .contains(a, b);
    }

    @Test
    void ignoresPropertiesNotAssignableToCaptureType() {
        // given - a component whose describeTo emits properties of unrelated types
        NoisyComponent noisy = new NoisyComponent();

        // when
        List<EventHandlingComponent> captured =
                CapturingComponentDescriptor.capturing(EventHandlingComponent.class).capture(noisy).toList();

        // then - only the EventHandlingComponent itself is captured; unrelated values are ignored
        assertThat(captured).containsExactly(noisy);
    }

    @Test
    void capturesEventHandlingComponentsInCollectionProperties() {
        // given - parent exposes children via a collection property
        StubEventHandlingComponent first = new StubEventHandlingComponent();
        StubEventHandlingComponent second = new StubEventHandlingComponent();
        CollectionParent parent = new CollectionParent(List.of(first, "ignored-string", second));

        // when
        List<EventHandlingComponent> captured =
                CapturingComponentDescriptor.capturing(EventHandlingComponent.class).capture(parent).toList();

        // then
        assertThat(captured).containsExactlyInAnyOrder(parent, first, second);
    }

    @Test
    void capturesEventHandlingComponentsInMapValueProperties() {
        // given - parent exposes children via a map property
        StubEventHandlingComponent first = new StubEventHandlingComponent();
        StubEventHandlingComponent second = new StubEventHandlingComponent();
        MapParent parent = new MapParent(java.util.Map.of("a", first, "b", second));

        // when
        List<EventHandlingComponent> captured =
                CapturingComponentDescriptor.capturing(EventHandlingComponent.class).capture(parent).toList();

        // then
        assertThat(captured).containsExactlyInAnyOrder(parent, first, second);
    }

    @Test
    void doesNotRecurseIntoPropertiesOfOtherDescribableTypes() {
        // given - a component that exposes a different DescribableComponent type as a property
        OtherKindComponent other = new OtherKindComponent();
        ParentExposingOtherKind parent = new ParentExposingOtherKind(other);

        // when - filter on EventHandlingComponent
        List<EventHandlingComponent> captured =
                CapturingComponentDescriptor.capturing(EventHandlingComponent.class).capture(parent).toList();

        // then - the other-kind property is not captured and is not recursed into
        assertThat(captured).containsExactly(parent);
    }

    @Test
    void usableDirectlyAsComponentDescriptorViaDescribeTo() {
        // given - a fresh descriptor and a decorator graph
        var descriptor = CapturingComponentDescriptor.capturing(EventHandlingComponent.class);
        StubEventHandlingComponent inner = new StubEventHandlingComponent();
        TestDecorator outer = new TestDecorator(inner);

        // when - drive the descriptor by hand, the way any DescribableComponent would
        outer.describeTo(descriptor);

        // then - inner is captured via the descriptor's describeProperty hook; outer itself is not
        // (describeTo walks the *contents* of a component, not the component itself)
        assertThat(descriptor.captured()).containsExactly(inner);
    }

    @Test
    void repeatedCaptureOnSameRootYieldsSameResults() {
        // given - a captor and a non-trivial graph: a decorator chain plus a collection child
        var captor = CapturingComponentDescriptor.capturing(EventHandlingComponent.class);
        StubEventHandlingComponent leaf = new StubEventHandlingComponent();
        TestDecorator middle = new TestDecorator(leaf);
        StubEventHandlingComponent sibling = new StubEventHandlingComponent();
        CollectionParent root = new CollectionParent(List.of(middle, sibling));

        // when - capture the same root twice on the same descriptor
        List<EventHandlingComponent> first = captor.capture(root).toList();
        List<EventHandlingComponent> second = captor.capture(root).toList();

        // then - both calls produce the same set: the identity-tracked set deduplicates the re-walk
        assertThat(first).containsExactlyInAnyOrder(root, middle, leaf, sibling);
        assertThat(second).containsExactlyInAnyOrderElementsOf(first);
    }

    private static class StubEventHandlingComponent implements EventHandlingComponent {

        @Override
        public MessageStream.@NonNull Empty<Message> handle(@NonNull EventMessage event,
                                                            @NonNull ProcessingContext context) {
            return MessageStream.empty();
        }

        @NonNull
        @Override
        public Object sequenceIdentifierFor(@NonNull EventMessage event, @NonNull ProcessingContext context) {
            return "stub";
        }

        @Override
        public MessageStream.@NonNull Empty<Message> handle(@NonNull ResetContext resetContext,
                                                            @NonNull ProcessingContext context) {
            return MessageStream.empty();
        }

        @Override
        public MessageStream.@NonNull Empty<Message> handle(@NonNull ReplayStatusChanged statusChange,
                                                            @NonNull ProcessingContext context) {
            return MessageStream.empty();
        }

        @NonNull
        @Override
        public Set<QualifiedName> supportedEvents() {
            return Set.of();
        }

        @Override
        public void describeTo(@NonNull ComponentDescriptor descriptor) {
            // leaf
        }
    }

    private static class TestDecorator extends DelegatingEventHandlingComponent {

        TestDecorator(EventHandlingComponent delegate) {
            super(delegate);
        }
    }

    private static class TestTwoChildDecorator extends StubEventHandlingComponent {

        private final EventHandlingComponent left;
        private final EventHandlingComponent right;

        TestTwoChildDecorator(EventHandlingComponent left, EventHandlingComponent right) {
            this.left = left;
            this.right = right;
        }

        @Override
        public void describeTo(@NonNull ComponentDescriptor descriptor) {
            descriptor.describeProperty("left", left);
            descriptor.describeProperty("right", right);
        }
    }

    private static class CycleComponent extends StubEventHandlingComponent {

        CycleComponent peer;

        @Override
        public void describeTo(@NonNull ComponentDescriptor descriptor) {
            descriptor.describeProperty("peer", peer);
        }
    }

    private static class NoisyComponent extends StubEventHandlingComponent {

        @Override
        public void describeTo(@NonNull ComponentDescriptor descriptor) {
            descriptor.describeProperty("name", "noisy");
            descriptor.describeProperty("count", 42L);
            descriptor.describeProperty("enabled", true);
            descriptor.describeProperty("object", new Object());
            descriptor.describeProperty("nullObj", (Object) null);
            descriptor.describeProperty("nullColl", (java.util.Collection<?>) null);
            descriptor.describeProperty("nullMap", (java.util.Map<?, ?>) null);
            descriptor.describeProperty("nullStr", (String) null);
        }
    }

    private static class CollectionParent extends StubEventHandlingComponent {

        private final List<?> children;

        CollectionParent(List<?> children) {
            this.children = children;
        }

        @Override
        public void describeTo(@NonNull ComponentDescriptor descriptor) {
            descriptor.describeProperty("children", children);
        }
    }

    private static class MapParent extends StubEventHandlingComponent {

        private final java.util.Map<?, ?> children;

        MapParent(java.util.Map<?, ?> children) {
            this.children = children;
        }

        @Override
        public void describeTo(@NonNull ComponentDescriptor descriptor) {
            descriptor.describeProperty("children", children);
        }
    }

    private static class OtherKindComponent implements org.axonframework.common.infra.DescribableComponent {

        @Override
        public void describeTo(@NonNull ComponentDescriptor descriptor) {
            throw new AssertionError("describeTo should not be invoked on a non-captured type");
        }
    }

    private static class ParentExposingOtherKind extends StubEventHandlingComponent {

        private final OtherKindComponent other;

        ParentExposingOtherKind(OtherKindComponent other) {
            this.other = other;
        }

        @Override
        public void describeTo(@NonNull ComponentDescriptor descriptor) {
            descriptor.describeProperty("other", other);
        }
    }
}
