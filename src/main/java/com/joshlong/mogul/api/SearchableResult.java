package com.joshlong.mogul.api;

import java.util.Date;
import java.util.Map;

/**
 * a searchable, ready to index or to show as a search result. {@code mogulId} is the
 * owner: every entry in the index carries it, and every search is filtered by it.
 */
public record SearchableResult<T extends Searchable>(long searchableId, T searchable, String title, String text,
		long aggregateId, Date created, String type, Map<String, Object> context, Long mogulId) {

	public SearchableResult(long searchableId, T searchable, String title, String text, long aggregateId, Date created,
			String type, Long mogulId) {
		this(searchableId, searchable, title, text, aggregateId, created, type,
				Map.of("type", type, "id", searchableId), mogulId);
	}

}
