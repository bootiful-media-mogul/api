package com.joshlong.mogul.api.compositions;

import com.joshlong.mogul.api.managedfiles.ManagedFile;

import java.io.Serializable;

public interface CompositionService {

	/**
	 * this is meant to be unique for a given entity, a field, and an id. so if you call
	 * this method and pass in keys that already exist this will fetch the existing
	 * composition, not create another one.
	 */
	Composition compose(Long mogulId, Class<?> clazz, Serializable publicationKey, String field);

	Composition getCompositionById(Long id);

	Attachment attach(Long compositionId, String key, ManagedFile managedFile);

}
