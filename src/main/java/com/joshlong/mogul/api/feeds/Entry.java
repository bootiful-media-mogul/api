package com.joshlong.mogul.api.feeds;

import java.time.Instant;
import java.util.Map;

public record Entry(String id, Instant updated, String title, String url, String summary,
		Map<String, String> metadata) {
}
