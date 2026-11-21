package com.joshlong.mogul.api;

import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * a title search the way a picker wants it: every word of the query somewhere in the
 * title, in any order and any case. the clip apps use it to find one post among a blog's
 * thousands, or one episode among a podcast's, without loading them all.
 *
 * @param sql {@code and} clauses to append to a {@code where}, one per word
 * @param params the clauses' parameters, in order
 */
public record TitleSearch(String sql, List<Object> params) {

	/**
	 * @param column the title column, as the surrounding sql names it
	 * @param query what was typed; blank matches everything
	 */
	public static TitleSearch of(String column, @Nullable String query) {
		var sql = new StringBuilder();
		var params = new ArrayList<Object>();
		if (query != null)
			for (var word : Arrays.stream(query.trim().split("\\s+")).filter(w -> !w.isEmpty()).toList()) {
				sql.append(" and ").append(column).append(" ilike ? escape '\\' ");
				params.add("%" + escape(word) + "%");
			}
		return new TitleSearch(sql.toString(), List.copyOf(params));
	}

	// a % or _ typed into the search is meant literally, not as a wildcard
	private static String escape(String word) {
		return word.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_");
	}

}
