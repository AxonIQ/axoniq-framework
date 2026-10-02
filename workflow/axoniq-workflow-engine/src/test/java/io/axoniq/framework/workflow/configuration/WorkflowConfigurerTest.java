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
package io.axoniq.framework.workflow.configuration;

import org.axonframework.common.configuration.ApplicationConfigurerTestSuite;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test for the {@link WorkflowConfigurer}.
 * <p>
 * {@link WorkflowConfigurer#create()} is not a "blank" {@link org.axonframework.common.configuration.ApplicationConfigurer}:
 * it registers a default pooled-streaming event-processing module as soon as it is built. That default module makes
 * several of the inherited {@link ApplicationConfigurerTestSuite} assertions, which assume a configurer contributes
 * no implicit state of its own, invalid as written. This class adjusts those assertions through the suite's
 * {@link #baselineModuleCount()} and {@link #expectedEnhancerInvocationCount()} extension points, and overrides the
 * handful of ordering-sensitive tests that cannot be expressed through a simple count.
 *
 * @author Simon Zambrovski
 */
class WorkflowConfigurerTest extends ApplicationConfigurerTestSuite<WorkflowConfigurer> {

    @Override
    public WorkflowConfigurer createConfigurer() {
        return WorkflowConfigurer.create();
    }

    @Override
    protected int baselineModuleCount() {
        return 3;
    }

    @Override
    protected int expectedEnhancerInvocationCount() {
        return 4;
    }

    @Nested
    class Defaults {

        @Test
        void createRegistersDefaultWorkflowEventProcessingModules() {
            Configuration configuration = buildConfiguration();

            assertEquals(baselineModuleCount(), configuration.getModuleConfigurations().size());
        }

        @Test
        void startSucceedsWithDefaultWorkflowEventProcessingModulesRegistered() {
            assertDoesNotThrow(() -> testSubject.start());
        }
    }

    @Nested
    class EnhancerRegistration extends ApplicationConfigurerTestSuite<WorkflowConfigurer>.EnhancerRegistration {

        @Override
        @Test
        protected void dynamicallyRegisteredEnhancersAreInvokedInCorrectOrder() {
            // given...
            List<String> executionOrder = new ArrayList<>();

            ConfigurationEnhancer enhancerC = new ConfigurationEnhancer() {
                @Override
                public void enhance(@NonNull ComponentRegistry registry) {
                    executionOrder.add("C");
                    registry.registerComponent(TestComponent.class, "C", c -> TestComponent.of("C"));
                }

                @Override
                public int order() {
                    return 5;
                }
            };

            ConfigurationEnhancer enhancerB = new ConfigurationEnhancer() {
                @Override
                public void enhance(@NonNull ComponentRegistry registry) {
                    executionOrder.add("B");
                    registry.registerComponent(TestComponent.class, "B", c -> TestComponent.of("B"));
                }

                @Override
                public int order() {
                    return 10;
                }
            };

            ConfigurationEnhancer enhancerA = new ConfigurationEnhancer() {
                @Override
                public void enhance(@NonNull ComponentRegistry registry) {
                    executionOrder.add("A");
                    registry.registerComponent(TestComponent.class, "A", c -> TestComponent.of("A"));
                    // Register enhancerC with order=5, which should be invoked before enhancerB (order=10)
                    registry.registerEnhancer(enhancerC);
                }

                @Override
                public int order() {
                    return 0;
                }
            };

            testSubject.componentRegistry(cr -> cr.registerEnhancer(enhancerA).registerEnhancer(enhancerB));

            // when...
            Configuration config = buildConfiguration();

            // then...
            assertEquals(TestComponent.of("A"), config.getComponent(TestComponent.class, "A"));
            assertEquals(TestComponent.of("B"), config.getComponent(TestComponent.class, "B"));
            assertEquals(TestComponent.of("C"), config.getComponent(TestComponent.class, "C"));

            // Every enhancer runs once per implicit module level, on top of the root.
            assertEquals(expectedEnhancerInvocationCount(), executionOrder.stream().filter("A"::equals).count());
            assertEquals(expectedEnhancerInvocationCount(), executionOrder.stream().filter("B"::equals).count());
            assertEquals(expectedEnhancerInvocationCount(), executionOrder.stream().filter("C"::equals).count());
            // Relative order is preserved regardless of how many times each enhancer additionally fires.
            assertTrue(executionOrder.indexOf("A") < executionOrder.indexOf("C"),
                       "EnhancerA (order=0) should execute before EnhancerC (order=5)");
            assertTrue(executionOrder.indexOf("C") < executionOrder.indexOf("B"),
                       "EnhancerC (order=5) should execute before EnhancerB (order=10)");
        }

        @Override
        @Test
        protected void dynamicallyRegisteredEnhancerWithLowerOrderThanParentExecutesAfterParent() {
            // given...
            List<String> executionOrder = new ArrayList<>();

            ConfigurationEnhancer childEnhancer = new ConfigurationEnhancer() {
                @Override
                public void enhance(@NonNull ComponentRegistry registry) {
                    executionOrder.add("child");
                    registry.registerComponent(TestComponent.class, "child", c -> TestComponent.of("child"));
                }

                @Override
                public int order() {
                    return 5; // Lower order than parent
                }
            };

            ConfigurationEnhancer parentEnhancer = new ConfigurationEnhancer() {
                @Override
                public void enhance(@NonNull ComponentRegistry registry) {
                    executionOrder.add("parent");
                    registry.registerComponent(TestComponent.class, "parent", c -> TestComponent.of("parent"));
                    // Register child with order=5, which is lower than parent's order=10
                    // But child should still execute AFTER parent since parent already executed
                    registry.registerEnhancer(childEnhancer);
                }

                @Override
                public int order() {
                    return 10; // Higher order than child
                }
            };

            testSubject.componentRegistry(cr -> cr.registerEnhancer(parentEnhancer));

            // when...
            Configuration config = buildConfiguration();

            // then...
            assertEquals(TestComponent.of("parent"), config.getComponent(TestComponent.class, "parent"));
            assertEquals(TestComponent.of("child"), config.getComponent(TestComponent.class, "child"));

            assertEquals(expectedEnhancerInvocationCount(), executionOrder.stream().filter("parent"::equals).count());
            assertEquals(expectedEnhancerInvocationCount(), executionOrder.stream().filter("child"::equals).count());
            // The first execution of parent must precede the first execution of the child it dynamically
            // registers, even though child's order value is lower.
            assertTrue(executionOrder.indexOf("parent") < executionOrder.indexOf("child"),
                       "Parent enhancer (order=10) should execute before the child (order=5) it dynamically "
                               + "registers");
        }

        @Nested
        class DisableEnhancer extends ApplicationConfigurerTestSuite<WorkflowConfigurer>.EnhancerRegistration.DisableEnhancer {

            @Override
            @Test
            protected void disableEnhancerWhenEnhancerWithLowerOrderDisablesHigherOrder() {
                // given...
                AtomicInteger lowOrderFireCount = new AtomicInteger(0);
                AtomicBoolean highOrderInvoked = new AtomicBoolean(false);

                class HighOrderEnhancer implements ConfigurationEnhancer {

                    @Override
                    public void enhance(@NonNull ComponentRegistry registry) {
                        highOrderInvoked.set(true);
                        registry.registerComponent(TestComponent.class, "high", c -> TestComponent.of("high"));
                    }

                    @Override
                    public int order() {
                        return 100; // Higher order, executes later
                    }
                }

                class LowOrderEnhancer implements ConfigurationEnhancer {

                    @Override
                    public void enhance(@NonNull ComponentRegistry registry) {
                        lowOrderFireCount.incrementAndGet();
                        registry.registerComponent(TestComponent.class, "low", c -> TestComponent.of("low"));
                        // Disable an enhancer that hasn't executed yet
                        registry.disableEnhancer(HighOrderEnhancer.class);
                    }

                    @Override
                    public int order() {
                        return 10; // Lower order, executes first
                    }
                }

                testSubject.componentRegistry(cr -> cr.registerEnhancer(new HighOrderEnhancer())
                                                      .registerEnhancer(new LowOrderEnhancer()));

                // when...
                Configuration config = buildConfiguration();

                // then...
                assertEquals(expectedEnhancerInvocationCount(), lowOrderFireCount.get(),
                             "LowOrderEnhancer (order=10) should execute once per implicit module level");
                assertFalse(highOrderInvoked.get(),
                            "HighOrderEnhancer (order=100) should NOT execute because it was disabled");
                assertEquals(TestComponent.of("low"), config.getComponent(TestComponent.class, "low"));
                assertFalse(config.getOptionalComponent(TestComponent.class, "high").isPresent(),
                            "HighOrderEnhancer's component should not exist");
            }

            @Override
            @Test
            protected void disableEnhancerCannotDisableAlreadyExecutedEnhancer() {
                // given...
                AtomicInteger lowOrderFireCount = new AtomicInteger(0);
                AtomicInteger highOrderFireCount = new AtomicInteger(0);

                class LowOrderEnhancer implements ConfigurationEnhancer {

                    @Override
                    public void enhance(@NonNull ComponentRegistry registry) {
                        lowOrderFireCount.incrementAndGet();
                        registry.registerComponent(TestComponent.class, "low", c -> TestComponent.of("low"));
                    }

                    @Override
                    public int order() {
                        return 10; // Lower order, executes first
                    }
                }

                class HighOrderEnhancer implements ConfigurationEnhancer {

                    @Override
                    public void enhance(@NonNull ComponentRegistry registry) {
                        highOrderFireCount.incrementAndGet();
                        registry.registerComponent(TestComponent.class, "high", c -> TestComponent.of("high"));
                        // Try to disable an enhancer that already executed; this has no effect.
                        registry.disableEnhancer(LowOrderEnhancer.class);
                    }

                    @Override
                    public int order() {
                        return 100; // Higher order, executes later
                    }
                }

                testSubject.componentRegistry(cr -> cr.registerEnhancer(new LowOrderEnhancer())
                                                      .registerEnhancer(new HighOrderEnhancer()));

                // when...
                Configuration config = buildConfiguration();

                // then...
                // Both enhancers keep executing every implicit module level: disabling an already-executed
                // enhancer never retroactively prevents it from having run, nor does it affect later levels.
                assertEquals(expectedEnhancerInvocationCount(), lowOrderFireCount.get());
                assertEquals(expectedEnhancerInvocationCount(), highOrderFireCount.get());
                assertEquals(TestComponent.of("low"), config.getComponent(TestComponent.class, "low"));
                assertEquals(TestComponent.of("high"), config.getComponent(TestComponent.class, "high"));
            }

            @Override
            @Test
            protected void disableEnhancerMultipleTimes() {
                // given...
                AtomicInteger firstEnhancerFireCount = new AtomicInteger(0);
                AtomicBoolean targetEnhancerInvoked = new AtomicBoolean(false);

                class TargetEnhancer implements ConfigurationEnhancer {

                    @Override
                    public void enhance(@NonNull ComponentRegistry registry) {
                        targetEnhancerInvoked.set(true);
                        registry.registerComponent(TestComponent.class, "target", c -> TestComponent.of("target"));
                    }

                    @Override
                    public int order() {
                        return 100; // Execute after others
                    }
                }

                ConfigurationEnhancer firstEnhancer = registry -> {
                    firstEnhancerFireCount.incrementAndGet();
                    // Disable multiple times - should work the same as disabling once.
                    registry.disableEnhancer(TargetEnhancer.class);
                    registry.disableEnhancer(TargetEnhancer.class.getName());
                    registry.disableEnhancer(TargetEnhancer.class);
                    registry.registerComponent(TestComponent.class, "first", c -> TestComponent.of("first"));
                };

                testSubject.componentRegistry(cr -> cr.registerEnhancer(firstEnhancer)
                                                      .registerEnhancer(new TargetEnhancer()));

                // when...
                Configuration config = buildConfiguration();

                // then...
                assertEquals(expectedEnhancerInvocationCount(), firstEnhancerFireCount.get(),
                             "First enhancer should execute once per implicit module level");
                assertFalse(targetEnhancerInvoked.get(),
                            "TargetEnhancer should NOT execute (disabled by multiple calls)");
                assertEquals(TestComponent.of("first"), config.getComponent(TestComponent.class, "first"));
                assertFalse(config.getOptionalComponent(TestComponent.class, "target").isPresent(),
                            "TargetEnhancer's component should not exist");
            }
        }
    }
}
