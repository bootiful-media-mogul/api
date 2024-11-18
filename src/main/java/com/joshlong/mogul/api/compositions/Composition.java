package com.joshlong.mogul.api.compositions;

import java.util.Collection;

/**
 * Represents textual content that can have associated managed files, links, etc.
 */
public record Composition(Long id, String payload, Class<?> payloadClass, String field,
		Collection<Attachment> attachments) {
}
