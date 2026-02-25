package io.axoniq.dataprotection.sample.giftcard.event

/**
 * Constants for gift card personal data encryption groups.
 *
 * Defines the group name and prefix used for GDPR key management,
 * allowing selective deletion of encrypted fields.
 *
 */
object GiftPersonalDataGroup {
    const val GROUP_NAME = "gift"
    const val GROUP_PREFIX = "gift-"
}