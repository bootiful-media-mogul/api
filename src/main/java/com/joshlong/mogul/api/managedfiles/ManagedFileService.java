package com.joshlong.mogul.api.managedfiles;

import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.http.MediaType;

import java.io.File;
import java.util.Collection;
import java.util.Map;

public interface ManagedFileService {

	// some files can be referenced externally, and they are said to be {@code visible}.
	void setManagedFileVisibility(Long managedFileId, boolean visible);

	Collection<ManagedFileDeletionRequest> getOutstandingManagedFileDeletionRequests();

	ManagedFileDeletionRequest getManagedFileDeletionRequestById(Long managedFileDeletionRequestId);

	void completeManagedFileDeletion(Long managedFileDeletionRequestId);

	void deleteManagedFile(Long managedFileId);

	ManagedFile getManagedFileById(Long managedFileId);

	Map<Long, ManagedFile> getManagedFiles(Collection<Long> managedFileIds);

	Resource read(Long managedFileId);

	/**
	 * the bytes from {@code start} to {@code end}, both inclusive, as an HTTP range is
	 */
	Resource read(Long managedFileId, long start, long end);

	void write(Long managedFileId, String filename, MediaType mts, Resource resource);

	/**
	 * the counterpart to {@link #write}, for bytes that something else put in S3 -- the
	 * {@code processors} module writes its results straight to the output object, so
	 * there is nothing to upload here, only a record to reconcile with what is now
	 * actually in storage. publishes the same {@link ManagedFileUpdatedEvent} that
	 * {@link #write} does, so everything downstream of a write behaves identically
	 * whichever process did the writing.
	 * @param mediaType the type the bytes were written as, which is not necessarily the
	 * type they arrived as: normalization turns a wav into an mp3
	 */
	void refreshManagedFileFromStorage(Long managedFileId, String filename, MediaType mediaType);

	/**
	 * behind the scenes this variant simply creates a {@link FileSystemResource} which
	 * can be queried for its content length
	 */
	void write(Long managedFileId, String filename, MediaType mts, File resource);

	String getPrivateUrlForManagedFile(Long managedFile);

	String getPublicUrlForManagedFile(Long managedFile);

	/**
	 * a managed file's storage key never changes: one produced-audio object is
	 * overwritten in place every time its episode is re-rendered. a bare public URL is
	 * therefore indistinguishable from the URL of the generation before it, and a CDN (or
	 * a browser) will go on serving the bytes it already has. the etag moves with every
	 * write, so pinning it to the query string makes each generation a resource of its
	 * own.
	 */
	String getVersionedPublicUrlForManagedFile(Long managedFile);

	/**
	 * the {@link #getVersionedPublicUrlForManagedFile versioned} URL, marked so that
	 * following it downloads the file rather than displaying it. {@code null} for a file
	 * with nothing written to it, or one that isn't publicly visible: there is nothing to
	 * hand out in either case.
	 */
	String getDownloadableUrlForManagedFile(Long managedFile);

	ManagedFile createManagedFile(Long mogulId, String folder, String fileName, long size, MediaType mediaType,
			boolean visible);

}
