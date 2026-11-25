package com.joshlong.mogul.api.managedfiles;

import com.joshlong.mogul.api.ApiProperties;
import com.joshlong.mogul.storage.Storage;
import com.joshlong.mogul.utils.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.Cache;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.SqlArrayValue;
import org.springframework.modulith.events.ApplicationModuleListener;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;
import org.springframework.web.util.UriComponentsBuilder;

import java.io.File;
import java.net.URI;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * the {@link ManagedFile managedFile} abstraction is used all over the place in the
 * system, and so this class provides caching. Lookups for a given {@link ManagedFile}
 * <em>should</em> be served out of in-memory cache first. This can spare you many one-off
 * calls to the database for every, say, podcast loaded in the system.
 */

class DefaultManagedFileService implements ManagedFileService {

	private final Cache cache;

	private final String bucket;

	private final Logger log = LoggerFactory.getLogger(getClass());

	private final ManagedFileDeletionRequestRowMapper managedFileDeletionRequestRowMapper = new ManagedFileDeletionRequestRowMapper();

	private final ManagedFileRowMapper managedFileRowMapper = new ManagedFileRowMapper();

	private final JdbcClient db;

	private final Storage storage;

	private final ApplicationEventPublisher publisher;

	private final TransactionTemplate transactionTemplate;

	private final URI cloudfrontDomain;

	private final boolean trace = this.log.isTraceEnabled();

	private final AtomicInteger counter = new AtomicInteger(0);

	DefaultManagedFileService(String bucket, JdbcClient db, Storage storage, ApplicationEventPublisher publisher,
			Cache cache, TransactionTemplate transactionTemplate, URI cloudfrontDomain, ApiProperties properties) {
		this.bucket = bucket;
		this.db = db;
		this.cloudfrontDomain = cloudfrontDomain;
		this.storage = storage;
		this.publisher = publisher;
		this.transactionTemplate = transactionTemplate;
		this.cache = cache;
		this.log.debug(
				"the file ManagedFile file system S3 bucket is called [{}] and the visible bucket is called [{}]",
				bucket, visibleBucketFor(bucket));

	}

	static String visibleBucketFor(String bucket) {
		return bucket + "-visible";
	}

	@Override
	public void write(Long managedFileId, String filename, MediaType mts, File resource) {
		this.write(managedFileId, filename, mts, new FileSystemResource(resource));
	}

	@Override
	public String getPrivateUrlForManagedFile(Long managedFileId) {
		var managedFile = this.getManagedFileById(managedFileId);
		return "/managedfiles/" + managedFile.id();
	}

	@Override
	public String getPublicUrlForManagedFile(Long managedFileId) {
		var managedFile = this.getManagedFileById(managedFileId);
		var url = (String) null;
		if (managedFile.visible()) {
			url = this.cloudfrontDomain.toString() + "/"
					+ this.fqn(managedFile.folder(), managedFile.storageFilename());
		}
		return url;
	}

	@Override
	public String getVersionedPublicUrlForManagedFile(Long managedFileId) {
		var url = this.getPublicUrlForManagedFile(managedFileId);
		if (url == null)
			return null;
		var version = version(this.getManagedFileById(managedFileId));
		if (version == null)
			return url;
		return UriComponentsBuilder.fromUriString(url).queryParam("v", version).toUriString();
	}

	@Override
	public String getDownloadableUrlForManagedFile(Long managedFileId) {
		var managedFile = this.getManagedFileById(managedFileId);
		if (managedFile == null || !managedFile.written() || !managedFile.visible())
			return null;
		var url = this.getVersionedPublicUrlForManagedFile(managedFileId);
		if (url == null)
			return null;
		return UriComponentsBuilder.fromUriString(url).queryParam("download", "true").toUriString();
	}

	/**
	 * S3 hands etags back quoted, and a multipart upload's carries a {@code -<parts>}
	 * suffix. the quotes have no business in a URL; what's left is hex and a dash, which
	 * needs no encoding. a file written before we started recording etags has none, and
	 * goes unversioned rather than unfetchable.
	 */
	private static String version(ManagedFile managedFile) {
		if (managedFile == null || !StringUtils.hasText(managedFile.etag()))
			return null;
		return managedFile.etag().replace("\"", "");
	}

	@ApplicationModuleListener
	void onManagedFileUpdated(ManagedFileUpdatedEvent event) {
		var managedFile = event.managedFile();
		this.ensureVisibility(managedFile);
	}

	private void ensureVisibility(ManagedFile managedFile) {
		var visibleBucket = managedFile.visibleBucket();
		var folder = managedFile.folder();
		var fn = managedFile.storageFilename();
		var fqn = this.fqn(folder, fn);
		var bucket = managedFile.bucket();
		if (managedFile.visible()) {
			if (this.storage.exists(bucket, fqn)) {
				var newContentType = StringUtils.hasText(managedFile.contentType())
						? MediaType.parseMediaType(managedFile.contentType()) : null;
				this.storage.copy(bucket, visibleBucket, fqn, newContentType);
			} //
			else {
				this.log.warn("the file {} does not exist and so can't be synced over!", fqn(bucket, fqn));
			}
		} //
		else {
			this.storage.remove(visibleBucket, fqn);
		}
		this.invalidateCache(managedFile.id());
	}

	@Override
	public void write(Long managedFileId, String filename, MediaType mediaType, Resource resource) {
		var managedFile = this.getManagedFileById(managedFileId);
		var bucket = managedFile.bucket();
		var folder = managedFile.folder();
		var fqn = this.fqn(folder, managedFile.storageFilename());
		this.storage.write(bucket, fqn, resource, mediaType);
		var clientMediaType = mediaType == null ? CommonMediaTypes.BINARY : mediaType;
		this.db //
			.sql("update managed_file set filename = ?, content_type = ?, written = true, size = ?, etag = ? where id= ?") //
			.params(filename, clientMediaType.toString(), contentLength(resource), this.etag(bucket, fqn),
					managedFileId) //
			.update();
		this.invalidateCache(managedFileId);
		var freshManagedFile = this.getManagedFileById(managedFileId);
		this.transactionTemplate.execute(_ -> {
			this.publisher.publishEvent(new ManagedFileUpdatedEvent(freshManagedFile));
			return null;
		});
	}

	@Override
	public void refreshManagedFileFromStorage(Long managedFileId, String filename, MediaType mediaType) {
		var managedFile = this.getManagedFileById(managedFileId);
		var fqn = this.fqn(managedFile.folder(), managedFile.storageFilename());
		var bucket = managedFile.bucket();
		// don't take the writer's word for it. a reply that says the work succeeded and a
		// bucket that has the bytes in it are two different claims, and only the second
		// one is worth marking the file written over.
		Assert.state(this.storage.exists(bucket, fqn),
				() -> "the object [" + bucket + "/" + fqn + "] for ManagedFile #" + managedFileId + " is not in S3");
		var clientMediaType = mediaType == null ? CommonMediaTypes.BINARY : mediaType;
		var metadata = this.storage.metadata(bucket, fqn);
		this.db //
			.sql("update managed_file set filename = ?, content_type = ?, written = true, size = ?, etag = ? where id= ?") //
			.params(filename, clientMediaType.toString(), metadata.contentLength(), metadata.etag(), managedFileId) //
			.update();
		this.invalidateCache(managedFileId);
		var freshManagedFile = this.getManagedFileById(managedFileId);
		this.transactionTemplate.execute(_ -> {
			this.publisher.publishEvent(new ManagedFileUpdatedEvent(freshManagedFile));
			return null;
		});
	}

	private void invalidateCache(Long managedFileId) {
		this.cache.evictIfPresent(managedFileId);
	}

	@Override
	public void setManagedFileVisibility(Long managedFileId, boolean publicAccess) {
		this.db.sql("update managed_file set visible = ? where id = ?").params(publicAccess, managedFileId).update();
		this.invalidateCache(managedFileId);
		this.ensureVisibility(this.getManagedFileById(managedFileId));
	}

	/**
	 * this one pulls down a {@link ManagedFile managed file}'s contents from S3, and then
	 * re-writes it, allowing us to synchronize our view of the S3 asset with the actual
	 * state of the S3 object.
	 */
	@Override
	public ManagedFileDeletionRequest getManagedFileDeletionRequestById(Long managedFileDeletionRequestId) {
		return db.sql("select * from managed_file_deletion_request where id =? ")
			.param(managedFileDeletionRequestId)
			.query(this.managedFileDeletionRequestRowMapper)
			.single();
	}

	@Override
	public Collection<ManagedFileDeletionRequest> getOutstandingManagedFileDeletionRequests() {
		return this.db//
			.sql("select * from managed_file_deletion_request where deleted = false")//
			.query(this.managedFileDeletionRequestRowMapper)//
			.list();
	}

	@Override
	public void completeManagedFileDeletion(Long managedFileDeletionRequestId) {
		var managedFileDeletionRequest = this.getManagedFileDeletionRequestById(managedFileDeletionRequestId);
		Assert.notNull(managedFileDeletionRequest, "the managed file deletion request should not be null");
		var fqn = this.fqn(managedFileDeletionRequest.folder(), managedFileDeletionRequest.storageFilename());
		for (var bucket : new String[] { managedFileDeletionRequest.bucket(),
				managedFileDeletionRequest.visibleBucket() }) {
			this.storage.remove(bucket, fqn);
		}
		this.db //
			.sql(" update managed_file_deletion_request set deleted = true where id = ? ") //
			.param(managedFileDeletionRequestId) //
			.update();
	}

	@Override
	@Transactional
	public void deleteManagedFile(Long managedFileId) {
		var managedFile = this.getManagedFileById(managedFileId);
		// very important that we do this part FIRST since the calls to mogulId will
		// lazily trigger the loading of the managed_file, which will be deleted if we
		// waited until after the next line
		this.db.sql(
				"insert into managed_file_deletion_request ( mogul_id , bucket, folder, filename ,storage_filename) values(?,?,?,?,?)")
			.params(managedFile.mogulId(), //
					managedFile.bucket(), //
					managedFile.folder(), //
					managedFile.filename(), //
					managedFile.storageFilename() //
			)
			.update();
		this.db.sql("delete from managed_file where id = ?").param(managedFileId).update();
		this.invalidateCache(managedFileId);
		this.publisher.publishEvent(new ManagedFileDeletedEvent(managedFile));
	}

	private String fqn(String folder, String filename) {
		return folder + '/' + filename;
	}

	@Override
	public Resource read(Long managedFileId) {
		var mf = this.getManagedFileById(managedFileId);
		var fn = this.fqn(mf.folder(), mf.storageFilename());
		var bucket = mf.bucket();
		return this.storage.read(bucket, fn);
	}

	@Override
	public Resource read(Long managedFileId, long start, long end) {
		var mf = this.getManagedFileById(managedFileId);
		return this.storage.read(mf.bucket(), this.fqn(mf.folder(), mf.storageFilename()), start, end);
	}

	/**
	 * the identifier for the bytes we just wrote, which is what lets work keyed to this
	 * file tell whether it has already been done. worth one HEAD: the alternative is
	 * paying for a whole transcription to find out nothing changed.
	 */
	private String etag(String bucket, String fqn) {
		try {
			return this.storage.metadata(bucket, fqn).etag();
		} //
		catch (Throwable throwable) {
			// a missing etag costs a redundant transcription, not a wrong one, so it is
			// not worth failing a completed upload over.
			this.log.warn("could not read the etag for [{}/{}]", bucket, fqn, throwable);
			return null;
		}
	}

	private long contentLength(Resource resource) {
		try {
			return resource.contentLength();
		} //
		catch (Throwable throwable) {
			return 0;
		}
	}

	@Override
	@Transactional
	public ManagedFile createManagedFile(Long mogulId, String folder, String fileName, long size, MediaType mediaType,
			boolean visible) {
		var kh = new GeneratedKeyHolder();
		var sql = """
				insert into managed_file( storage_filename, mogul_id , bucket, folder, filename, size,content_type, visible)
				values (?,?,?,?,?,?,?,?)
				""";
		this.db.sql(sql)
			.params(UUID.randomUUID().toString(), mogulId, bucket, folder, fileName, size, mediaType.toString(),
					visible)
			.update(kh);
		var mf = this.getManagedFileById(((Number) Objects.requireNonNull(kh.getKeys()).get("id")).longValue());
		if (visible)
			this.ensureVisibility(mf);
		return mf;
	}

	@Override
	public ManagedFile getManagedFileById(Long managedFileId) {
		if (managedFileId == null)
			return null;
		var all = this.getManagedFiles(List.of(managedFileId));
		return CollectionUtils.firstOrNull(all.values());
	}

	private void debug() {
		if (this.trace) {
			var calls = new StringBuilder();
			StackWalker.getInstance().forEach(stackFrame -> calls.append(stackFrame.toString()).append("\n"));
			this.log.info("called getManagedFiles {} times. stack trace for readThrough: {}", counter.incrementAndGet(),
					calls);
		}
	}

	@Override
	public Map<Long, ManagedFile> getManagedFiles(Collection<Long> managedFileIds) {
		this.debug();
		var outcome = new HashMap<Long, ManagedFile>();
		var everythingElse = new ArrayList<Long>();
		for (var mfId : managedFileIds) {
			if (mfId == null || mfId <= 0)
				continue;
			var entry = this.cache.get(mfId, ManagedFile.class);
			if (entry != null)
				outcome.put(mfId, entry);
			else
				everythingElse.add(mfId);
		}
		// sparing this trip is the entire point of the cache, and it wasn't being
		// spared: `id = any('{}')` finds nothing, but it still goes to the database and
		// back to find it.
		if (everythingElse.isEmpty())
			return outcome;
		var results = db.sql("select * from managed_file where id = any(?)")
			.params(new SqlArrayValue("bigint", everythingElse.toArray()))
			.query(this.managedFileRowMapper)
			.list();
		for (var r : results) {
			this.cache.put(r.id(), r);
			outcome.put(r.id(), r);
		}
		return outcome;
	}

}
