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

package io.axoniq.framework.messaging;

import com.tngtech.archunit.core.domain.Dependency;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchCondition;
import com.tngtech.archunit.lang.ArchRule;
import com.tngtech.archunit.lang.ConditionEvents;
import com.tngtech.archunit.lang.SimpleConditionEvent;
import org.junit.jupiter.api.*;

import java.lang.invoke.MethodHandles;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static com.tngtech.archunit.library.dependencies.SlicesRuleDefinition.slices;
import static org.assertj.core.api.Assertions.assertThat;

@AnalyzeClasses(packages = ArchUnitPackageRulesTest.BASE_PACKAGE_NAME)
public class ArchUnitPackageRulesTest {

    public static final String BASE_PACKAGE_NAME = "io.axoniq.framework.messaging";

    @ArchTest
    private final ArchRule packagesShouldBeFreeOfCycles = slices()
            .matching("(**)")
            .should()
            .beFreeOfCycles()
            .as("Package Cycles");

    /**
     * General form of ArchUnit's {@code NO_CLASSES_SHOULD_DEPEND_UPPER_PACKAGES} that permits depending on
     * upper-package <em>interfaces</em>. Implementations in component-owned sub-packages (e.g. the tracing decorators
     * in {@code ..distributed.tracing}) must implement the contract of the package they decorate, so a dependency on
     * an upper-package interface is legitimate; depending on upper-package <em>classes</em> remains forbidden.
     */
    @ArchTest
    private final ArchRule noClassesShouldDependOnUpperPackages = noClasses()
            .should(dependOnUpperPackagesExceptInterfaces())
            .as("Package Hierarchy Violations");

    private static ArchCondition<JavaClass> dependOnUpperPackagesExceptInterfaces() {
        return new ArchCondition<>("depend on upper packages (interfaces excluded)") {
            @Override
            public void check(JavaClass javaClass, ConditionEvents events) {
                for (Dependency dependency : javaClass.getDirectDependenciesFromSelf()) {
                    JavaClass target = dependency.getTargetClass().getBaseComponentType();
                    boolean upperPackage = javaClass.getPackageName()
                                                    .startsWith(target.getPackageName() + ".");
                    if (upperPackage && !target.isInterface()) {
                        events.add(SimpleConditionEvent.satisfied(dependency, dependency.getDescription()));
                    }
                }
            }
        };
    }

    @Test
    void shouldMatchPackageName() {
        assertThat(MethodHandles.lookup().lookupClass().getPackageName()).isEqualTo(BASE_PACKAGE_NAME);
    }
}
