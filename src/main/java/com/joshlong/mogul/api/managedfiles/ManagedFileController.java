package com.joshlong.mogul.api.managedfiles;

import com.joshlong.mogul.api.mogul.MogulService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;
import org.springframework.graphql.data.method.annotation.Argument;
import org.springframework.graphql.data.method.annotation.MutationMapping;
import org.springframework.graphql.data.method.annotation.QueryMapping;
import org.springframework.graphql.data.method.annotation.SchemaMapping;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.util.Assert;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.util.Map;

@Controller
class ManagedFileController {

	private static final String PUBLIC_MF_URL = "/public/managedfiles/{id}";

	private static final String MF_RW_URL = "/managedfiles/{id}";

	private final Logger log = LoggerFactory.getLogger(getClass());

	private final ManagedFileService managedFileService;

	private final MogulService mogulService;

	ManagedFileController(ManagedFileService managedFileService, MogulService mogulService) {
		this.managedFileService = managedFileService;
		this.mogulService = mogulService;
	}

	@MutationMapping
	boolean setManagedFileVisibility(@Argument Long managedFileId, @Argument Boolean visible) {
		this.ownedManagedFile(managedFileId);
		this.managedFileService.setManagedFileVisibility(managedFileId, visible);
		return true;
	}

	@SchemaMapping
	String visibleUrl(ManagedFile managedFile) {
		return this.managedFileService.getPublicUrlForManagedFile(managedFile.id());
	}

	@SchemaMapping
	String downloadableUrl(ManagedFile managedFile) {
		return this.managedFileService.getDownloadableUrlForManagedFile(managedFile.id());
	}

	@SchemaMapping
	String url(ManagedFile managedFile) {
		return this.managedFileService.getPrivateUrlForManagedFile(managedFile.id());
	}

	@QueryMapping
	ManagedFile managedFileById(@Argument Long managedFileId) {
		return this.ownedManagedFile(managedFileId);
	}

	// ids are sequential, so they're easy to guess: check it's the signed-in mogul's
	private ManagedFile ownedManagedFile(Long managedFileId) {
		var mogulId = this.mogulService.getCurrentMogul().id();
		var managedFile = managedFileId == null ? null : this.managedFileService.getManagedFileById(managedFileId);
		Assert.state(managedFile != null && mogulId.equals(managedFile.mogulId()),
				() -> "there is no managed file #" + managedFileId + " for mogul #" + mogulId);
		return managedFile;
	}

	@ResponseBody
	@GetMapping(PUBLIC_MF_URL)
	ResponseEntity<Resource> readPublic(@PathVariable Long id,
			@RequestHeader(value = HttpHeaders.RANGE, required = false) String range) {
		return this.doRead(true, id, range);
	}

	@ResponseBody
	@GetMapping(MF_RW_URL)
	ResponseEntity<Resource> read(@PathVariable Long id,
			@RequestHeader(value = HttpHeaders.RANGE, required = false) String range) {
		return this.doRead(false, id, range);
	}

	private ResponseEntity<Resource> doRead(boolean assertVisible, Long managedFileId, String rangeHeader) {
		Assert.notNull(managedFileId, "the managedFileId is null");
		var managedFile = this.managedFileService.getManagedFileById(managedFileId);
		Assert.notNull(managedFile, "the managed file does not exist [" + managedFileId + "]");
		if (assertVisible) {
			if (!managedFile.visible()) {
				this.log.warn("someone is attempting to read managed file #{} even though it's not visible publicly",
						managedFile.id());
				return ResponseEntity.notFound().build();
			}
		} //
		else if (!managedFile.mogulId().equals(this.mogulService.getCurrentMogul().id())) {
			// the private url is for the file's owner. anyone else gets the same answer
			// as
			// for a file that doesn't exist, so ids can't be probed.
			this.log.warn("mogul #{} attempted to read managed file #{}, which belongs to mogul #{}",
					this.mogulService.getCurrentMogul().id(), managedFile.id(), managedFile.mogulId());
			return ResponseEntity.notFound().build();
		}
		var contentType = managedFile.contentType();
		this.log.debug("content-type: {}", contentType);
		var size = managedFile.size();
		var range = ByteRange.of(rangeHeader, size);
		if (range == null) {
			return ResponseEntity.ok()
				.header(HttpHeaders.ACCEPT_RANGES, "bytes")
				.contentLength(size)
				.contentType(MediaType.parseMediaType(contentType))
				.body(this.managedFileService.read(managedFileId));
		}
		if (range == ByteRange.UNSATISFIABLE) {
			return ResponseEntity.status(HttpStatus.REQUESTED_RANGE_NOT_SATISFIABLE)
				.header(HttpHeaders.CONTENT_RANGE, "bytes */" + size)
				.build();
		}
		return ResponseEntity.status(HttpStatus.PARTIAL_CONTENT)
			.header(HttpHeaders.ACCEPT_RANGES, "bytes")
			.header(HttpHeaders.CONTENT_RANGE, range.contentRange())
			.contentLength(range.length())
			.contentType(MediaType.parseMediaType(contentType))
			.body(this.managedFileService.read(managedFileId, range.start(), range.end()));
	}

	@ResponseBody
	@PostMapping(MF_RW_URL)
	Map<String, Object> write(@PathVariable Long id, @RequestParam MultipartFile file) {
		Assert.notNull(id, "the id should not be null");
		var mogul = this.mogulService.getCurrentMogul();
		var managedFile = this.managedFileService.getManagedFileById(id);
		Assert.notNull(managedFile, "the managed file is null for managed file id [" + id + "]");
		Assert.state(managedFile.mogulId().equals(mogul.id()),
				"you're trying to write to an invalid file to which you are not authorized!");
		var managedFileId = managedFile.id();
		var originalFilename = file.getOriginalFilename();
		var mediaType = CommonMediaTypes.guess(file.getResource());
		log.debug("guessing the media type for [{}] is  {}", file.getOriginalFilename(), mediaType);
		this.managedFileService.write(managedFileId, originalFilename, mediaType, file.getResource());
		var updated = this.managedFileService.getManagedFileById(managedFileId);
		this.log.trace("finished writing managed file [{}] to s3: {}:{}", id, originalFilename, updated.toString());
		return Map.of("managedFileId", id);
	}

}
