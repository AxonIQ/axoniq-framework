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
package io.axoniq.workflow.springboot;

import io.axoniq.workflow.runtime.api.WorkflowContext;
import io.axoniq.workflow.springboot.SpringUtils.WorkflowBeanDefinition;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for sorting workflow beans by their {@link Order} annotation.
 */
@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = SpringUtilsSortByOrderTest.TestConfig.class)
public class SpringUtilsSortByOrderTest {

    @Autowired
    private ConfigurableListableBeanFactory beanFactory;

    @Test
    void shouldSortByOrder() {
        WorkflowBeanDefinition first = new WorkflowBeanDefinition("firstBean",
                                                                  FirstBean.class,
                                                                  WorkflowContext.class);
        WorkflowBeanDefinition second = new WorkflowBeanDefinition("secondBean",
                                                                   SecondBean.class,
                                                                   WorkflowContext.class);
        WorkflowBeanDefinition last = new WorkflowBeanDefinition("lastBean",
                                                                 LastBean.class,
                                                                 WorkflowContext.class);
        WorkflowBeanDefinition unordered = new WorkflowBeanDefinition("unorderedBean",
                                                                      UnorderedBean.class,
                                                                      WorkflowContext.class);

        List<WorkflowBeanDefinition> unsorted = Arrays.asList(unordered, last, second, first);
        List<WorkflowBeanDefinition> sorted = SpringUtils.sortByOrder(unsorted, beanFactory);

        assertThat(sorted).containsExactly(first, second, last, unordered);
    }

    @Configuration
    static class TestConfig {

        @Bean
        public FirstBean firstBean() {
            return new FirstBean();
        }

        @Bean
        public SecondBean secondBean() {
            return new SecondBean();
        }

        @Bean
        public LastBean lastBean() {
            return new LastBean();
        }

        @Bean
        public UnorderedBean unorderedBean() {
            return new UnorderedBean();
        }
    }

    @Order(1)
    static class FirstBean {

    }

    @Order(2)
    static class SecondBean {

    }

    @Order(100)
    static class LastBean {

    }

    static class UnorderedBean {

    }
}
