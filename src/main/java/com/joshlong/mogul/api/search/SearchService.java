package com.joshlong.mogul.api.search;

import com.joshlong.mogul.api.Searchable;
import com.joshlong.mogul.api.SearchableResult;

import java.util.Collection;
import java.util.Map;

public interface SearchService {

	/**
	 * writes the searchable's entry, or removes it if the searchable is gone or has
	 * nothing to index.
	 */
	void index(Searchable searchable);

	void delete(Class<? extends Searchable> type, Long searchableId);

	/**
	 * the mogul's own things that match, best first. nobody else's are ever considered.
	 */
	<T extends Searchable> Collection<SearchableResult<T>> search(Long mogulId, String shouldContain,
			Map<String, Object> metadata);

	/**
	 * throws away the mogul's entries, and any left over from before entries had owners,
	 * and indexes everything the mogul owns again, from the database.
	 * @return how many entries were written
	 */
	int rebuild(Long mogulId);

}
