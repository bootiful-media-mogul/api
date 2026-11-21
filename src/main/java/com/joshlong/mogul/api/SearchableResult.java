package com.joshlong.mogul.api;

import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * a searchable, ready to index or to show as a search result. {@code mogulId} is the
 * owner: every entry in the index carries it, and every search is filtered by it. the
 * {@code context} names the result ({@code type}, {@code id}), and says what it belongs
 * to (a post's {@code blogId}, say), which a search's metadata can narrow results by.
 */
public record SearchableResult<T extends Searchable>(long searchableId, T searchable, String title, String text,
		long aggregateId, Date created, String type, Map<String, Object> context, Long mogulId) {

	public SearchableResult(long searchableId, T searchable, String title, String text, long aggregateId, Date created,
			String type, Long mogulId) {
		this(searchableId, searchable, title, text, aggregateId, created, type, mogulId, Map.of());
	}

	/**
	 * @param scope what the result belongs to, added to its context
	 */
	public SearchableResult(long searchableId, T searchable, String title, String text, long aggregateId, Date created,
			String type, Long mogulId, Map<String, Object> scope) {
		this(searchableId, searchable, title, text, aggregateId, created, type, contextFor(type, searchableId, scope),
				mogulId);
	}

	private static Map<String, Object> contextFor(String type, long searchableId, Map<String, Object> scope) {
		var context = new LinkedHashMap<String, Object>(scope);
		context.put("type", type);
		context.put("id", searchableId);
		return Map.copyOf(context);
	}

}
