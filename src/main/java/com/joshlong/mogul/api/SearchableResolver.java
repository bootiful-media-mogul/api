package com.joshlong.mogul.api;

import java.util.Collection;
import java.util.List;

public interface SearchableResolver<T extends Searchable> extends DomainResolver<Searchable, T> {

	/**
	 * the results for these ids, in one go where possible. ids that no longer resolve are
	 * left out, which is how search notices an index entry for something that's gone.
	 */
	List<SearchableResult<T>> results(List<Long> searchableIds);

	/**
	 * the ids of everything of this kind that the mogul owns, for rebuilding the mogul's
	 * part of the index from scratch.
	 */
	Collection<Long> searchableIds(Long mogulId);

}
