package com.joshlong.mogul.api.compositions;

public interface CompositionService {

	/**
	 * this is meant to be unique for a given entity, a field, and an id. so if you call
	 * this method and pass in keys that already exist this will fetch the existing
	 * composition, not create another one.
	 */
	<T extends Composable> Composition compose(T payload, String field);

	Attachment attach(Long compositionId, String caption, Long managedFileId);

}
