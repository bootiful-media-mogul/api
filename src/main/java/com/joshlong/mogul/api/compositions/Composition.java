package com.joshlong.mogul.api.compositions;

import java.util.Collection;

/**
 * Represents textual content that can have associated managed files, links, etc.
 */
public record Composition(Long id, String key, String field, Collection<Attachment> attachments) {
}
