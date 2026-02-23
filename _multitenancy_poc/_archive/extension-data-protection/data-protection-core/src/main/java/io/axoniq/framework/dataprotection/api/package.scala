/*
 * Copyright (c) 2017-2023. AxonIQ B.V.
 */
package io.axoniq.framework.dataprotection

import scala.annotation.meta.field

package object api {

  type dataSubjectId = DataSubjectId @field
  type personalData = PersonalData @field
  type deepPersonalData = DeepPersonalData @field
  type serializedPersonalData = SerializedPersonalData @field
  type personalDataType = PersonalDataType

}
