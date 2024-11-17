package com.joshlong.mogul.api.compositions;

import com.joshlong.mogul.api.managedfiles.ManagedFile;

/**
 * the composition may have 0..N attachments which in turn are just managed files with
 * logical, referenceable names.
 */
public record Attachment(Long id, String key, ManagedFile managedFile) {
}
