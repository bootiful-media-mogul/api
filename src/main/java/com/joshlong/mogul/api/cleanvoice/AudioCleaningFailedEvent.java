package com.joshlong.mogul.api.cleanvoice;

import java.util.Map;

/**
 * the audio couldn't be cleaned. {@code actionRequired} means it won't work until the
 * mogul does something -- fixes their api key, or buys more credit -- rather than that it
 * might work if tried again.
 */
public record AudioCleaningFailedEvent(Long mogulId, String reason, boolean actionRequired,
		Map<String, Object> context) {

}
