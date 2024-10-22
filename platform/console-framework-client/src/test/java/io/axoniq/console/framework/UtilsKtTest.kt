/*
 * Copyright (c) 2024. AxonIQ B.V.
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

package io.axoniq.console.framework

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class UtilsKtTest {
    @Test
    fun `Unwraps decorator with single field`() {
        val core = MyCoreDecoratableService()
        val decorated = MySingleValueDecoratableService(core)
        val unwrapped = decorated.unwrapPossiblyDecoratedClass(MyDecoratableService::class.java)
        assertEquals(core, unwrapped)
    }

    @Test
    fun `Unwraps decorator that has a nullable field out of two`() {
        val core = MyCoreDecoratableService()
        val decorated = MyDoubleNullableValueDecoratableService(core, null)
        val unwrapped = decorated.unwrapPossiblyDecoratedClass(MyDecoratableService::class.java)
        val decorated2 = MyDoubleNullableValueDecoratableService(null, core)
        val unwrapped2 = decorated2.unwrapPossiblyDecoratedClass(MyDecoratableService::class.java)
        assertEquals(core, unwrapped)
        assertEquals(core, unwrapped2)
    }

    @Test
    fun `Does not crash on null fields of single-field decorator and returns decorator itself`() {
        val decorated = MySingleValueDecoratableService(null)
        val unwrapped = decorated.unwrapPossiblyDecoratedClass(MyDecoratableService::class.java)
        assertEquals(decorated, unwrapped)
    }

    @Test
    fun `Does not crash on both null fields of double-field decorator and returns decorator itself`() {
        val decorated = MyDoubleNullableValueDecoratableService(null, null)
        val unwrapped = decorated.unwrapPossiblyDecoratedClass(MyDecoratableService::class.java)
        assertEquals(decorated, unwrapped)
    }

    interface MyDecoratableService {

    }

    class MyCoreDecoratableService : MyDecoratableService {

    }

    class MySingleValueDecoratableService(
            val delegate: MyDecoratableService?
    ) : MyDecoratableService


    class MyDoubleNullableValueDecoratableService(
            val delegate: MyDecoratableService?,
            val delegate2: MyDecoratableService?,
    ) : MyDecoratableService
}