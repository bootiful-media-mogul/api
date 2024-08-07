package com.joshlong.mogul.api.mogul;

/**
 * published when the {@link Mogul } has logged in
 *
 * @param mogul the mogul
 */
public record MogulAuthenticatedEvent(Mogul mogul) {
}
