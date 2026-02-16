package io.axoniq.example.workflow.kotlin

@JvmRecord
data class RegistrationReceivedEvent(val id: String, val email: String)
