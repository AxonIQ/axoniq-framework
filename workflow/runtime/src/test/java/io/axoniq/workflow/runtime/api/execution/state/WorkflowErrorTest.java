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
package io.axoniq.workflow.runtime.api.execution.state;

import org.axonframework.conversion.Converter;
import org.axonframework.conversion.TestConverter;
import org.axonframework.conversion.jackson2.Jackson2Converter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

class WorkflowErrorTest {

    @Test
    void fromReturnsNullForNullThrowable() {
        assertThat(WorkflowError.from(null)).isNull();
    }

    @Test
    void fromCapturesClassNameAndMessageOnly() {
        var error = WorkflowError.from(new IllegalStateException("nope"));
        assertThat(error).isNotNull();
        assertThat(error.type()).isEqualTo(IllegalStateException.class.getName());
        assertThat(error.message()).isEqualTo("nope");
        assertThat(error.cause()).isNull();
    }

    @Test
    void fromHandlesNullMessage() {
        var error = WorkflowError.from(new RuntimeException((String) null));
        assertThat(error).isNotNull();
        assertThat(error.message()).isNull();
    }

    @Test
    void fromTruncatesOversizedMessage() {
        String huge = "x".repeat(5_000);
        var error = WorkflowError.from(new RuntimeException(huge));
        assertThat(error).isNotNull();
        assertThat(error.message()).hasSize(WorkflowError.TRUNCATED_MESSAGE_SIZE);
    }

    @Test
    void fromKeepsShortMessageVerbatim() {
        var error = WorkflowError.from(new RuntimeException("short"));
        assertThat(error).isNotNull();
        assertThat(error.message()).isEqualTo("short");
    }

    @Test
    void fromWalksCauseChain() {
        var root = new IllegalArgumentException("root");
        var middle = new IllegalStateException("middle", root);
        var top = new RuntimeException("top", middle);

        var error = WorkflowError.from(top);

        assertThat(error.type()).isEqualTo(RuntimeException.class.getName());
        assertThat(error.cause()).isNotNull();
        assertThat(error.cause().type()).isEqualTo(IllegalStateException.class.getName());
        assertThat(error.cause().cause()).isNotNull();
        assertThat(error.cause().cause().type()).isEqualTo(IllegalArgumentException.class.getName());
        assertThat(error.cause().cause().cause()).isNull();
    }

    @Test
    void fromBreaksCycleInCauseChain() {
        class Looping extends RuntimeException {
            private Throwable loopTarget;
            Looping(String m) { super(m); }
            @Override public Throwable getCause() { return loopTarget; }
        }
        var a = new Looping("a");
        var b = new Looping("b");
        a.loopTarget = b;
        b.loopTarget = a;

        var error = WorkflowError.from(a);

        assertThat(error.type()).isEqualTo(Looping.class.getName());
        assertThat(error.cause()).isNotNull();
        assertThat(error.cause().type()).isEqualTo(Looping.class.getName());
        assertThat(error.cause().cause()).isNull();
    }

    @Test
    void fromBoundsDeepCauseChain() {
        Throwable current = new RuntimeException("leaf");
        for (int i = 0; i < 50; i++) {
            current = new RuntimeException("level-" + i, current);
        }

        var error = WorkflowError.from(current);

        int depth = 0;
        WorkflowError walker = error;
        while (walker != null) {
            depth++;
            walker = walker.cause();
        }
        assertThat(depth).isLessThanOrEqualTo(WorkflowError.MAX_CAUSE_DEPTH);
    }

    @Test
    void implementsCauseInterface() {
        Cause cause = WorkflowError.from(new IllegalStateException("boom"));
        assertThat(cause).isNotNull();
        assertThat(cause.type()).isEqualTo(IllegalStateException.class.getName());
        assertThat(cause.message()).isEqualTo("boom");
    }

    @Test
    void isTypeMatchesByFullyQualifiedName() {
        Cause cause = WorkflowError.from(new IllegalStateException("boom"));
        assertThat(cause.isType(IllegalStateException.class)).isTrue();
        assertThat(cause.isType(RuntimeException.class)).isFalse();
    }

    @Test
    void fromUnwrapsCauseTypeWhenThrowableImplementsCause() {
        var wrapped = new WorkflowExecutionException("com.acme.Boom", "boom",
                                                     new WorkflowExecutionException("com.acme.Inner", "inner", null));

        var error = WorkflowError.from(wrapped);

        assertThat(error).isNotNull();
        assertThat(error.type()).isEqualTo("com.acme.Boom");
        assertThat(error.message()).isEqualTo("boom");
        assertThat(error.cause()).isNotNull();
        assertThat(error.cause().type()).isEqualTo("com.acme.Inner");
        assertThat(error.cause().message()).isEqualTo("inner");
    }

    @Test
    void toThrowableProducesStacklessWorkflowExecutionException() {
        var error = WorkflowError.from(new IllegalStateException("boom"));

        Throwable reconstructed = error.toThrowable();

        assertThat(reconstructed).isInstanceOfSatisfying(WorkflowExecutionException.class, e -> {
            assertThat(e.type()).isEqualTo(IllegalStateException.class.getName());
            assertThat(e.getMessage()).isEqualTo("boom");
            assertThat(e.getStackTrace()).isEmpty();
        });
    }

    @Test
    void toThrowableRebuildsCauseChain() {
        var root = new IllegalArgumentException("root");
        var wrapped = new RuntimeException("wrap", root);

        Throwable reconstructed = WorkflowError.from(wrapped).toThrowable();

        assertThat(reconstructed.getCause()).isInstanceOfSatisfying(WorkflowExecutionException.class, e -> {
            assertThat(e.type()).isEqualTo(IllegalArgumentException.class.getName());
            assertThat(e.getMessage()).isEqualTo("root");
        });
    }

    @Test
    void roundTripThrowableThroughWorkflowErrorIsStable() {
        var original = new IllegalStateException("boom", new IOException("inner"));

        var error = WorkflowError.from(original);
        var fromError = error.toThrowable();
        var fromFromError = WorkflowError.from(fromError);

        assertThat(fromFromError).isEqualTo(error);
    }

    @Test
    void roundTripWorkflowExecutionExceptionThroughWorkflowErrorIsStable() {
        var original = new WorkflowExecutionException("com.acme.Boom", "msg",
                                                     new WorkflowExecutionException("com.acme.Inner", "i", null));

        var error = WorkflowError.from(original);
        var fromError = error.toThrowable();
        var fromFromError = WorkflowError.from(fromError);

        assertThat(fromFromError).isEqualTo(error);
    }

    @Test
    void roundTripWorkflowExecutionExceptionPreservesTypeAndMessageAcrossCauseChain() {
        var inner = new WorkflowExecutionException("com.acme.Inner", "i", null);
        var outer = new WorkflowExecutionException("com.acme.Boom", "msg", inner);

        var error = WorkflowError.from(outer);
        var reconstructed = error.toThrowable();

        assertThat(reconstructed.type()).isEqualTo(outer.type());
        assertThat(reconstructed.getMessage()).isEqualTo(outer.getMessage());
        assertThat(reconstructed.getCause()).isInstanceOfSatisfying(WorkflowExecutionException.class, c -> {
            assertThat(c.type()).isEqualTo(inner.type());
            assertThat(c.getMessage()).isEqualTo(inner.getMessage());
        });
    }

    @ParameterizedTest
    @EnumSource(value = TestConverter.class, names = {"JACKSON", "CBOR"})
    void converterRoundTripPreservesData(TestConverter testConverter) {
        var original = new RuntimeException("outer", new IllegalStateException("inner"));
        var compact = WorkflowError.from(original);

        WorkflowError roundTripped = testConverter.serializeDeserialize(compact);

        assertThat(roundTripped.type()).isEqualTo(RuntimeException.class.getName());
        assertThat(roundTripped.message()).isEqualTo("outer");
        assertThat(roundTripped.cause()).isNotNull();
        assertThat(roundTripped.cause().type()).isEqualTo(IllegalStateException.class.getName());
        assertThat(roundTripped.cause().message()).isEqualTo("inner");
    }

    @ParameterizedTest
    @EnumSource(value = TestConverter.class, names = {"JACKSON", "CBOR"})
    void converterPayloadStaysCompactForDeepStackTraces(TestConverter testConverter) {
        RuntimeException ex = new RuntimeException("boom");
        StackTraceElement[] frames = new StackTraceElement[200];
        for (int i = 0; i < frames.length; i++) {
            frames[i] = new StackTraceElement(
                    "com.example.very.long.package.path.ClassName" + i, "methodName" + i, "Source" + i + ".java", i);
        }
        ex.setStackTrace(frames);

        Converter converter = testConverter.getConverter();
        byte[] serialized = converter.convert(WorkflowError.from(ex), byte[].class);

        assertThat(serialized.length).isLessThan(500);
        assertThat(new String(serialized)).doesNotContain("com.example.very.long.package.path.ClassName");
    }

    @Test
    void jacksonWireFormatHasNoStackTraceField() {
        Converter converter = new Jackson2Converter();
        var compact = WorkflowError.from(new RuntimeException("boom"));

        String json = new String(converter.convert(compact, byte[].class));

        assertThat(json).doesNotContain("stackTrace");
    }
}
