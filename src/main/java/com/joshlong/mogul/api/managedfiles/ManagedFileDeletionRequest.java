package com.joshlong.mogul.api.managedfiles;

import java.util.Date;

import static com.joshlong.mogul.api.managedfiles.DefaultManagedFileService.visibleBucketFor;

public record ManagedFileDeletionRequest(Long id, Long mogulId, String bucket, String folder, String filename,
		String storageFilename, boolean deleted, Date created) {

	public String visibleBucket() {
		return visibleBucketFor(this.bucket);
	}
}
