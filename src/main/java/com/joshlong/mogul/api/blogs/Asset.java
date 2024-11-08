package com.joshlong.mogul.api.blogs;

import com.joshlong.mogul.api.managedfiles.ManagedFile;

public record Asset(Long postId, Long id, ManagedFile managedFile) {
}
