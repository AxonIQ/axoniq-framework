package io.axoniq.example.workflow.kotlin

import com.fasterxml.jackson.annotation.JsonProperty

@JvmRecord
data class MagicHappenedEvent(
    @field:JsonProperty("magician")
    val magician: String
)
