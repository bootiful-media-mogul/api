package com.joshlong.mogul.api.compositions;

import java.util.Collection;
import java.util.Map;

public interface CompositionService {

	/**
	 * this is meant to be unique for a given entity, a field, and an id. so if you call
	 * this method and pass in keys that already exist, this will fetch the existing
	 * composition, not create another one.
	 * <p>
	 * the class and the key are all a composition is ever identified by, so callers that
	 * have only an id don't need to go and load the entity to ask for one.
	 */
	Composition compose(Class<? extends Composable> payloadClass, Long compositionKey, String field);

	/**
	 * convenience for callers that already hold the entity. it is reduced to its class
	 * and its {@link Composable#compositionKey() key} either way -- nothing else about
	 * the object is read.
	 */
	default <T extends Composable> Composition compose(T payload, String field) {
		return this.compose(payload.getClass(), payload.compositionKey(), field);
	}

	Attachment createCompositionAttachment(Long mogulId, Long compositionId, String caption);

	Composition getCompositionById(Long id);

	Map<Long, Composition> getCompositionsByIds(Collection<Long> ids);

	void deleteCompositionById(Long id);

	void deleteCompositionAttachmentyId(Long id);

	/**
	 * the composition an attachment belongs to, or {@code null} if there's no such
	 * attachment.
	 */
	Composition getCompositionByAttachmentId(Long attachmentId);

	String createMarkdownPreviewForAttachment(Attachment attachment);

}
