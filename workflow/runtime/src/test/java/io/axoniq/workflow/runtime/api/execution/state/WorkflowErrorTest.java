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
import org.axonframework.conversion.jackson2.Jackson2Converter;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class WorkflowErrorTest {

    private final Converter converter = new Jackson2Converter();

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
    void jsonRoundTripDropsStackTraceAndShrinksPayload() {
        RuntimeException ex = new RuntimeException("boom");
        StackTraceElement[] frames = new StackTraceElement[200];
        for (int i = 0; i < frames.length; i++) {
            frames[i] = new StackTraceElement(
                    "com.example.very.long.package.path.ClassName" + i, "methodName" + i, "Source" + i + ".java", i);
        }
        ex.setStackTrace(frames);

        WorkflowError compact = WorkflowError.from(ex);

        byte[] serialized = converter.convert(compact, byte[].class);
        String json = new String(serialized);

        assertThat(json).doesNotContain("stackTrace")
                        .doesNotContain("com.example.very.long.package.path.ClassName");
        assertThat(serialized.length).isLessThan(200);

        WorkflowError roundTripped = converter.convert(serialized, WorkflowError.class);
        assertThat(roundTripped.type()).isEqualTo(RuntimeException.class.getName());
        assertThat(roundTripped.message()).isEqualTo("boom");
        assertThat(roundTripped.cause()).isNull();
    }

    @Test
    void jsonRoundTripPreservesCauseChain() {
        var original = new RuntimeException("outer", new IllegalStateException("inner"));

        byte[] serialized = converter.convert(WorkflowError.from(original), byte[].class);
        WorkflowError roundTripped = converter.convert(serialized, WorkflowError.class);

        assertThat(roundTripped.type()).isEqualTo(RuntimeException.class.getName());
        assertThat(roundTripped.message()).isEqualTo("outer");
        assertThat(roundTripped.cause()).isNotNull();
        assertThat(roundTripped.cause().type()).isEqualTo(IllegalStateException.class.getName());
        assertThat(roundTripped.cause().message()).isEqualTo("inner");
    }
}
