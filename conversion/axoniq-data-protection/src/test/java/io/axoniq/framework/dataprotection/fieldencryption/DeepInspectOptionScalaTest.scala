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
package io.axoniq.framework.dataprotection.fieldencryption

import io.axoniq.framework.dataprotection.api.{FieldEncrypter, dataSubjectId, deepPersonalData, personalData}
import io.axoniq.framework.dataprotection.cryptoengine.{CryptoEngine, InMemoryCryptoEngine}
import io.axoniq.framework.dataprotection.utils.TestUtils.{defaultTestConverter, isClear, isEncrypted}
import org.fluttercode.datafactory.impl.DataFactory
import org.junit.Assert.{assertEquals, assertFalse, assertTrue}
import org.junit.{Before, Test}
import org.slf4j.LoggerFactory

import java.lang.invoke.MethodHandles
import java.util.concurrent.ThreadLocalRandom

class DeepInspectOptionScalaTest {

  case class A(@dataSubjectId id: Long, @deepPersonalData b1: Option[B], b2: Option[B])

  case class B(@personalData x: String, @personalData y: Option[String], @deepPersonalData c: Option[C])

  case class C(@dataSubjectId id: Long, @personalData x: String)

  class X(@dataSubjectId val id: Long, @personalData val name: String, @deepPersonalData var x: Option[X] = null)

  private val logger = LoggerFactory.getLogger(MethodHandles.lookup.lookupClass)

  private var cryptoEngine: CryptoEngine = _
  private var fieldEncrypter: FieldEncrypter = _
  private var dataFactory: DataFactory = _

  @Before
  def setUp(): Unit = {
    cryptoEngine = new InMemoryCryptoEngine
    fieldEncrypter = new FieldEncrypter(cryptoEngine, defaultTestConverter())
    dataFactory = new DataFactory
    dataFactory.randomize(ThreadLocalRandom.current.nextInt)
  }

  @Test
  def deepInspectionIsBasedOnAnnotationWithFilledOptions(): Unit = {
    val b1: Option[B] = Some(B(dataFactory.getLastName, Some(dataFactory.getFirstName), None))
    val b2: Option[B] = Some(B(dataFactory.getCity, Some(dataFactory.getFirstName), None))

    val a = A(1L, b1, b2)

    logger.info("before encryption {}", a)
    fieldEncrypter.encrypt(a)
    logger.info("after encryption {}", a)

    a.b1.foreach((b: B) => assertTrue(isEncrypted(b.x)))
    assertTrue(a.b1.get.y.isDefined)
    a.b1.foreach((b: B) => assertTrue(isEncrypted(b.y.get)))
    a.b2.foreach((b: B) => assertTrue(isClear(b.x)))
    assertTrue(a.b2.get.y.isDefined)
    a.b2.foreach((b: B) => assertTrue(isClear(b.y.get)))

    fieldEncrypter.decrypt(a)
    a.b1.foreach((b: B) => assertTrue(isClear(b.x)))
    assertTrue(a.b1.get.y.isDefined)
    a.b1.foreach((b: B) => assertTrue(isClear(b.y.get)))
    a.b2.foreach((b: B) => assertTrue(isClear(b.x)))
    assertTrue(a.b2.get.y.isDefined)
    a.b2.foreach((b: B) => assertTrue(isClear(b.y.get)))
  }

  @Test
  def deepInspectionIsBasedOnAnnotationWithEmptyOptions(): Unit = {
    val b1: Option[B] = Some(B(dataFactory.getLastName, None, None))
    val b2: Option[B] = None

    val a = A(1L, b1, b2)

    logger.info("before encryption {}", a)
    fieldEncrypter.encrypt(a)
    logger.info("after encryption {}", a)

    assertTrue(a.b1.isDefined)
    a.b1.foreach((b: B) => assertTrue(isEncrypted(b.x)))
    assertFalse(a.b1.get.y.isDefined)
    assertFalse(a.b2.isDefined)

    fieldEncrypter.decrypt(a)
    assertTrue(a.b1.isDefined)
    a.b1.foreach((b: B) => assertTrue(isClear(b.x)))
    assertFalse(a.b1.get.y.isDefined)
    assertFalse(a.b2.isDefined)
  }

  @Test
  def deepInspectionMustWorkRecursively(): Unit = {
    val a = A(
      1L,
      Some(B(dataFactory.getLastName, Some(dataFactory.getFirstName), Some(C(1L, dataFactory.getFirstName)))),
      None
    )

    logger.info("before encryption {}", a)
    fieldEncrypter.encrypt(a)
    logger.info("after encryption {}", a)

    assertTrue(isEncrypted(a.b1.get.x))
    assertTrue(isEncrypted(a.b1.get.y.get))
    assertTrue(isEncrypted(a.b1.get.c.get.x))

    fieldEncrypter.decrypt(a)
    assertTrue(isClear(a.b1.get.x))
    assertTrue(isClear(a.b1.get.y.get))
    assertTrue(isClear(a.b1.get.c.get.x))
  }

  @Test
  def multipleKeysMustBeSupported(): Unit = {
    val lastName = dataFactory.getLastName
    val firstName = dataFactory.getLastName
    val a = A(1L, Some(B(lastName, Some(firstName), Some(C(2L, firstName)))), null)

    fieldEncrypter.encrypt(a)
    cryptoEngine.deleteKey("1")
    fieldEncrypter.decrypt(a)
    logger.info("after encryption, key deletion, decryption {}", a)

    assertEquals("", a.b1.get.x)
    assertEquals("", a.b1.get.y.get)
    assertEquals(firstName, a.b1.get.c.get.x)
  }

  @Test
  def cyclesShouldBeUnproblematic(): Unit = {
    val x1 = new X(1L, dataFactory.getLastName)
    val x2 = new X(2L, dataFactory.getLastName, Some(x1))
    val x3 = new X(3L, dataFactory.getLastName, Some(x2))
    x1.x = Some(x3)

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
