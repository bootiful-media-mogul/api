package com.joshlong.mogul.api.mogul;

import javax.annotation.Nullable;

/**
 * published when the {@link Mogul } has logged in
 *
 * @param mogul the mogul
 */
public record MogulAuthenticatedEvent(@Nullable Mogul mogul) {
}
