/*
 * Copyright (c) 2022-2026. AxonIQ B.V.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.axoniq.platform.framework.client

import io.axoniq.platform.framework.AxoniqPlatformConfiguration
import io.axoniq.platform.framework.api.ClientSettingsV2
import io.axoniq.platform.framework.api.CommandBusInformation
import io.axoniq.platform.framework.api.EventStoreInformation
import io.axoniq.platform.framework.api.ModuleVersion
import io.axoniq.platform.framework.api.QueryBusInformation
import io.axoniq.platform.framework.api.SetupPayload
import io.axoniq.platform.framework.api.Versions
import io.axoniq.platform.framework.client.strategy.CborEncodingStrategy
import io.mockk.every
import io.mockk.mockk
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.concurrent.TimeUnit

class AxoniqConsoleRSocketClientIntegrationTest {

    private lateinit var mockServer: MockConsoleServer
    private lateinit var client: AxoniqConsoleRSocketClient

    @BeforeEach
    fun setUp() {
        mockServer = MockConsoleServer()
        mockServer.start()
    }

    @AfterEach
    fun tearDown() {
        client.disposeClient()
        mockServer.stop()
    }

    // ---- tests ----

    @Test
    fun `connects successfully and receives settings`() {
        client = buildClient()
        client.start()

        await().atMost(5, TimeUnit.SECONDS).until { client.isConnected() }
    }

    @Test
    fun `does not connect with invalid authentication`() {
        mockServer.rejectSetup = true
        client = buildClient()
        client.start()

        // Give the client several retry cycles. It must never report connected.
        Thread.sleep(4000)
        assertFalse(client.isConnected())
    }

    @Test
    fun `reconnects after server closes connection`() {
        client = buildClient()
        client.start()

        await().atMost(5, TimeUnit.SECONDS).until { client.isConnected() }

        mockServer.disconnectClients()

        // Client should detect the TCP close and mark itself disconnected.
        await().atMost(5, TimeUnit.SECONDS).until { !client.isConnected() }

        // Backoff for first retry is 2^0 = 1 s; allow generous margin.
        await().atMost(10, TimeUnit.SECONDS).until { client.isConnected() }
    }

    @Test
    fun `disconnects when heartbeats from server stop arriving`() {
        mockServer.clientSettings = ClientSettingsV2(
                heartbeatInterval = 200,
                heartbeatTimeout = 1000,
                processorReportInterval = 5000,
                handlerReportInterval = 5000,
                applicationReportInterval = 5000,
        )
        client = buildClient()
        client.start()

        await().atMost(5, TimeUnit.SECONDS).until { client.isConnected() }

        mockServer.stopSendingHeartbeats()

        // heartbeatTimeout=1000ms, checker runs every 1000ms → disconnect within ~2000ms; allow 5s.
        await().atMost(5, TimeUnit.SECONDS).until { !client.isConnected() }
    }

    // ---- helpers ----

    private fun buildClient(): AxoniqConsoleRSocketClient {
        val encodingStrategy = CborEncodingStrategy()
        val setupPayloadCreator = mockk<SetupPayloadCreator>()
        every { setupPayloadCreator.createReport() } returns minimalSetupPayload()

        val config = AxoniqPlatformConfiguration("test-env", "test-token", "test-app")
                .host("localhost")
                .port(mockServer.port)
                .secure(false)

        return AxoniqConsoleRSocketClient(
                properties = config,
                setupPayloadCreator = setupPayloadCreator,
                registrar = RSocketHandlerRegistrar(encodingStrategy),
                encodingStrategy = encodingStrategy,
                clientSettingsService = ClientSettingsService(),
                instanceName = "test-instance"
        )
    }

    private fun minimalSetupPayload() = SetupPayload(
            commandBus = CommandBusInformation(
                    type = "test", axonServer = false, localSegmentType = null,
                    context = null, messageSerializer = null,
            ),
            queryBus = QueryBusInformation(
                    type = "test", axonServer = false, localSegmentType = null,
                    context = null, messageSerializer = null, serializer = null,
            ),
            eventStore = EventStoreInformation(
                    type = "test", axonServer = false, context = null,
                    eventSerializer = null, snapshotSerializer = null,
            ),
            processors = emptyList(),
            versions = Versions(frameworkVersion = "test", moduleVersions = emptyList<ModuleVersion>()),
            upcasters = emptyList(),
    )
}
