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
package io.axoniq.framework.extension.dataprotection.fieldencryption

import io.axoniq.framework.extension.dataprotection.api.{FieldEncrypter, dataSubjectId, deepPersonalData, personalData}
import io.axoniq.framework.extension.dataprotection.cryptoengine.{CryptoEngine, InMemoryCryptoEngine}
import io.axoniq.framework.extension.dataprotection.utils.TestUtils
import io.axoniq.framework.extension.dataprotection.utils.TestUtils.{defaultTestConverter, isClear, isEncrypted}
import org.fluttercode.datafactory.impl.DataFactory
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.{Before, Test}
import org.slf4j.LoggerFactory

import java.lang.invoke.MethodHandles
import java.util.concurrent.ThreadLocalRandom

class DeepInspectCollectionsScalaTest {

  case class A(@dataSubjectId id: Long, @deepPersonalData b1: Seq[B], b2: Seq[B])
  case class B(@personalData x: String, @deepPersonalData c: Seq[C])
  case class C(@dataSubjectId id: Long, @personalData x: String)
  class X(@dataSubjectId val id: Long, @personalData val name: String, @deepPersonalData var x: Seq[X] = null)

  private val logger = LoggerFactory.getLogger(MethodHandles.lookup.lookupClass)

  private var cryptoEngine: CryptoEngine = _
  private var fieldEncrypter: FieldEncrypter = _
  private var dataFactory: DataFactory = _

  @Before
  def setUp(): Unit = {
    cryptoEngine = new InMemoryCryptoEngine
    cryptoEngine.registerEntitlementManager(TestUtils.mockEntitlementManager)
    fieldEncrypter = new FieldEncrypter(cryptoEngine, defaultTestConverter())
    dataFactory = new DataFactory
    dataFactory.randomize(ThreadLocalRandom.current.nextInt)
  }

  @Test
  def deepInspectionIsBasedOnAnnotation(): Unit = {
    var b1 = List[B]()
    var b2 = List[B]()
    for (i <- 1 to 3) {
      b1 = B(dataFactory.getLastName, null) :: b1
      b2 = B(dataFactory.getCity, null) :: b2
    }
    val a = A(1L, b1, b2)
    logger.info("before encryption {}", a)
    fieldEncrypter.encrypt(a)
    logger.info("after encryption {}", a)
    a.b1.foreach((b: B) => assertTrue(isEncrypted(b.x)))
    a.b2.foreach((b: B) => assertTrue(isClear(b.x)))
    fieldEncrypter.decrypt(a)
    a.b1.foreach((b: B) => assertTrue(isClear(b.x)))
    a.b2.foreach((b: B) => assertTrue(isClear(b.x)))
  }

  @Test
  def deepInspectionMustWorkRecursively(): Unit = {
    val a = A(1L, B(dataFactory.getLastName, C(1L, dataFactory.getFirstName) :: Nil) :: Nil, null)
    logger.info("before encryption {}", a)
    fieldEncrypter.encrypt(a)
    logger.info("after encryption {}", a)
    assertTrue(isEncrypted(a.b1.head.x))
    assertTrue(isEncrypted(a.b1.head.c.head.x))
    fieldEncrypter.decrypt(a)
    assertTrue(isClear(a.b1.head.x))
    assertTrue(isClear(a.b1.head.c.head.x))
  }

  @Test
  def multipleKeysMustBeSupported(): Unit = {
    val lastname = dataFactory.getLastName
    val firstname = dataFactory.getLastName
    val a = A(1L, B(lastname, C(2L, firstname) :: Nil) :: Nil, null)
    fieldEncrypter.encrypt(a)
    cryptoEngine.deleteKey("1")
    fieldEncrypter.decrypt(a)
    logger.info("after encryption, key deletion, decryption {}", a)
    assertEquals("", a.b1.head.x)
    assertEquals(firstname, a.b1.head.c.head.x)
  }

  @Test
  def cyclesShouldBeUnproblematic(): Unit = {
    val x1 = new X(1L, dataFactory.getLastName)
    val x2 = new X(2L, dataFactory.getLastName, x1 :: Nil)
    val x3 = new X(3L, dataFactory.getLastName, x2 :: Nil)
    x1.x = x3 :: Nil
    fieldEncrypter.encrypt(x1)
    assertTrue(isEncrypted(x1.name))
    assertTrue(isEncrypted(x2.name))
    assertTrue(isEncrypted(x3.name))
    fieldEncrypter.decrypt(x2)
    assertTrue(isClear(x1.name))
    assertTrue(isClear(x2.name))
    assertTrue(isClear(x3.name))
  }
}
