package com.joshlong.mogul.api;

/**
 * a {@link Searchable} is gone, so its entry in the search index should go too.
 */
public record SearchableDeletedEvent(Class<? extends Searchable> type, Long searchableId) {
}
