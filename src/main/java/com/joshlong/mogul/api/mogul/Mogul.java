package com.joshlong.mogul.api.mogul;

/**
 * the main tenant/user of this system.
 */
public record Mogul(Long id, String username, String email, String clientId) {
}
