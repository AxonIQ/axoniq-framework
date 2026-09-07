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
package io.axoniq.framework.workflow.springboot.kotlin

import io.axoniq.framework.workflow.dsl.kotlin.Kontext
import io.axoniq.framework.workflow.dsl.kotlin.WorkflowKontext
import io.axoniq.framework.workflow.runtime.api.annotation.Workflow
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowDefinition
import io.axoniq.framework.workflow.runtime.execution.WorkflowConfigurationRegistry
import org.assertj.core.api.Assertions.assertThat
import org.axonframework.eventsourcing.eventstore.EventStorageEngine
import org.axonframework.eventsourcing.eventstore.inmemory.InMemoryEventStorageEngine
import org.axonframework.messaging.core.QualifiedName
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.inmemory.InMemoryTokenStore
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.autoconfigure.EnableAutoConfiguration
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.EnableMBeanExport
import org.springframework.jmx.support.RegistrationPolicy
import org.springframework.test.context.ContextConfiguration

@SpringBootTest(
    classes = [KotlinWorkflowAutodetectionIT.TestConfig::class],
    webEnvironment = SpringBootTest.WebEnvironment.NONE,
    properties = ["axon.axonserver.enabled=false"]
)
class KotlinWorkflowAutodetectionIT {

    @Autowired
    private lateinit var registry: WorkflowConfigurationRegistry<*>

    @Autowired
    private lateinit var kotlinWorkflow: KotlinWorkflow

    @Test
    fun `should autodetect kotlin workflow`() {
        val configurations = registry.getWorkflowsConfigurations(QualifiedName("io.namespace.KotlinEvent"))
        assertThat(configurations).isNotNull
        assertThat(configurations).hasSize(1)
        val conf = configurations.first()
        assertThat(conf.configuration().workflowName()).isEqualTo("KotlinWorkflow")

        (conf.configuration()
            .workflowDefinition() as WorkflowDefinition<WorkflowKontext>).accept(mock(WorkflowKontext::class.java))
        assertThat(kotlinWorkflow.executed).isTrue()
    }

    @ContextConfiguration
    @EnableAutoConfiguration
    @EnableMBeanExport(registration = RegistrationPolicy.IGNORE_EXISTING)
    class TestConfig {

        @Bean
        fun kotlinWorkflow() = KotlinWorkflow()

        @Bean
        fun tokenStore(): TokenStore {
            return InMemoryTokenStore()
        }

        @Bean
        fun eventStorageEngine(): EventStorageEngine {
            return InMemoryEventStorageEngine()
        }
    }

}

class KotlinWorkflow {
    var executed = false

    @Workflow(
        workflowName = "KotlinWorkflow",
        startOnEventName = "io.namespace.KotlinEvent"
    )
    fun Kontext.onExecute() {
        executed = true
    }

    fun KotlinWorkflow.execute(kontext: Kontext) = with(kontext) { onExecute() }
}
