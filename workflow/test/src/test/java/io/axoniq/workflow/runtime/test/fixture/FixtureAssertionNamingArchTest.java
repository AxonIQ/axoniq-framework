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
package io.axoniq.workflow.runtime.test.fixture;

import com.tngtech.archunit.base.DescribedPredicate;
import com.tngtech.archunit.core.domain.JavaMethod;
import com.tngtech.archunit.core.domain.JavaModifier;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchCondition;
import com.tngtech.archunit.lang.ArchRule;
import com.tngtech.archunit.lang.ConditionEvents;
import com.tngtech.archunit.lang.SimpleConditionEvent;

import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.methods;

/**
 * ArchUnit tests for fixture assertion naming conventions.
 *
 * @author Simon Zambrovski
 */
@AnalyzeClasses(packagesOf = {WorkflowTestDriver.class, WorkflowEngineTestingState.class})
class FixtureAssertionNamingArchTest {

    private static final Set<String> TARGET_CLASSES = Set.of(
            WorkflowTestDriver.class.getName(),
            WorkflowEngineTestingState.class.getName()
    );

    @ArchTest
    static final ArchRule satisfiesMethodsUseVoidAndConsumer =
            methods().that(arePublicMethodsDeclaredInTargetClasses())
                     .and().haveNameMatching(".*Satisfies$")
                     .should(haveRawReturnType(void.class))
                     .andShould(haveParameterOfType(Consumer.class));

    @ArchTest
    static final ArchRule matchesMethodsReturnValueAndUsePredicate =
            methods().that(arePublicMethodsDeclaredInTargetClasses())
                     .and().haveNameMatching(".*Matches$")
                     .should(notHaveRawReturnType(void.class))
                     .andShould(haveParameterOfType(Predicate.class));

    @ArchTest
    static final ArchRule existsMethodsReturnValueAndTakeNoArguments =
            methods().that(arePublicMethodsDeclaredInTargetClasses())
                     .and().haveNameMatching(".*Exists$")
                     .should(notHaveRawReturnType(void.class))
                     .andShould(haveNoParameters());

    @ArchTest
    static final ArchRule publicFixtureAssertionApiDoesNotExposeAssertPrefixedMethods =
            methods().that(arePublicMethodsDeclaredInTargetClasses())
                     .should().haveNameNotMatching("^assert[A-Z]*");

    private static DescribedPredicate<JavaMethod> arePublicMethodsDeclaredInTargetClasses() {
        return new DescribedPredicate<>("public methods declared in fixture assertion types") {
            @Override
            public boolean test(JavaMethod method) {
                return method.getModifiers().contains(JavaModifier.PUBLIC)
                        && TARGET_CLASSES.contains(method.getOwner().getName());
            }
        };
    }

    private static ArchCondition<JavaMethod> haveParameterOfType(Class<?> type) {
        return new ArchCondition<>("have a parameter of type %s".formatted(type.getSimpleName())) {
            @Override
            public void check(JavaMethod method, ConditionEvents events) {
                boolean matches = method.getRawParameterTypes()
                                        .stream()
                                        .anyMatch(parameterType -> parameterType.isEquivalentTo(type));
                events.add(new SimpleConditionEvent(method, matches,
                                                    "%s does not declare a parameter of type %s"
                                                            .formatted(method.getFullName(), type.getSimpleName())));
            }
        };
    }

    private static ArchCondition<JavaMethod> haveRawReturnType(Class<?> type) {
        return new ArchCondition<>("have raw return type %s".formatted(type.getSimpleName())) {
            @Override
            public void check(JavaMethod method, ConditionEvents events) {
                boolean matches = method.getRawReturnType().isEquivalentTo(type);
                events.add(new SimpleConditionEvent(method, matches,
                                                    "%s has raw return type %s instead of %s"
                                                            .formatted(method.getFullName(),
                                                                       method.getRawReturnType().getSimpleName(),
                                                                       type.getSimpleName())));
            }
        };
    }

    private static ArchCondition<JavaMethod> notHaveRawReturnType(Class<?> type) {
        return new ArchCondition<>("not have raw return type %s".formatted(type.getSimpleName())) {
            @Override
            public void check(JavaMethod method, ConditionEvents events) {
                boolean matches = !method.getRawReturnType().isEquivalentTo(type);
                events.add(new SimpleConditionEvent(method, matches,
                                                    "%s has raw return type %s"
                                                            .formatted(method.getFullName(), type.getSimpleName())));
            }
        };
    }

    private static ArchCondition<JavaMethod> haveNoParameters() {
        return new ArchCondition<>("have no parameters") {
            @Override
            public void check(JavaMethod method, ConditionEvents events) {
                boolean matches = method.getRawParameterTypes().isEmpty();
                events.add(new SimpleConditionEvent(method, matches,
                                                            "%s declares %d parameters"
                                                                    .formatted(method.getFullName(),
                                                                               method.getRawParameterTypes().size())));
            }
        };
    }
}
