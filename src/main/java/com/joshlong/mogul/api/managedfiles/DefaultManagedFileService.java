package com.joshlong.mogul.api.managedfiles;

import com.joshlong.mogul.api.utils.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.event.EventListener;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.Assert;
import org.springframework.util.FileCopyUtils;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * the {@link ManagedFile managedFile} abstraction is used all over the place in the
 * system, and so this class provides caching. Lookups for a given {@link ManagedFile}
 * <em>should</em> be served out of in-memory cache first. This can spare you thousands of
 * one-off calls to the database for every, say, podcast loaded in the system.
 *
 * <h2>Performance Optimizations to be Aware of for Managed Files</h2> <EM>Important!</EM>
 * the {@link ManagedFile managed file} abstraction is used everywhere, and we can't
 * afford a million calls to the SQL table for each single {@link ManagedFile} required in
 * some object in a deep object graph of results. So, we use transaction synchronization
 * to wait until the end of a given transaction, including read-only transactions, to make
 * note of all the ids of the managed files and then to hand the user code back a
 * placeholder {@link ManagedFile} which knows only its ID. It has the shape of a managed
 * file, but not the data of one. that is until the transaction finishes. at this point,
 * right before committing , we do one big query for all the managed files and then give
 * the data to each placeholder object, fleshing them out, in effect.
 */

class TemporaryVisibilityMigration {

	void migrateBucket(S3Client s3Client, String sourceBucket, String targetBucket, String key, Region region) {

		// First, create the new bucket if it doesn't exist
		if (!this.bucketExists(s3Client, targetBucket)) {
			var createBucketRequest = CreateBucketRequest.builder().bucket(targetBucket).build();
			s3Client.createBucket(createBucketRequest);
			System.out.println("Created target bucket: " + targetBucket);
		}

		var copyRequest = CopyObjectRequest.builder()
			.sourceBucket(sourceBucket)
			.sourceKey(key)
			.destinationBucket(targetBucket)
			.destinationKey(key)
			.build();
		s3Client.copyObject(copyRequest);

	}

	private boolean bucketExists(S3Client s3, String bucketName) {
		try {
			s3.headBucket(HeadBucketRequest.builder().bucket(bucketName).build());
			return true;
		} //
		catch (S3Exception e) {
			return false;
		}
	}

}

@Service
class DefaultManagedFileService implements ManagedFileService {

	private final Logger log = LoggerFactory.getLogger(getClass());

	private final ManagedFileDeletionRequestRowMapper managedFileDeletionRequestRowMapper = new ManagedFileDeletionRequestRowMapper();

	private final ThreadLocal<Map<Long, ManagedFile>> managedFiles = new ThreadLocal<>();

	private final JdbcClient db;

	private final Storage storage;

	private final ApplicationEventPublisher publisher;

	private final TransactionTemplate transactionTemplate;

	// todo delete the variable s3 it's only to support the migration!
	private final S3Client s3;

	// todo delete this method its only here to support the migration!
	@EventListener
	void migrateEverything(ApplicationReadyEvent readyEvent) throws Exception {

		var temporaryVisibilityMigration = new TemporaryVisibilityMigration();

		record DumbManagedFile(long id, String bucket, String folder, String storageFilename) {
		}

		class DumbManagedFileRowMapper implements RowMapper<DumbManagedFile> {

			@Override
			public DumbManagedFile mapRow(ResultSet rs, int rowNum) throws SQLException {
				return new DumbManagedFile(rs.getLong("id"), rs.getString("bucket"), rs.getString("folder"),
						rs.getString("storage_filename"));
			}

		}

		this.db.sql("select * from managed_file where visible = true")
			.query(new DumbManagedFileRowMapper())
			.stream()
			.forEach(dmf -> {
				var visibleBucket = visibleBucketFor(dmf.bucket());
				var sourceBucket = dmf.bucket();
				var fqn = fqn(dmf.folder(), dmf.storageFilename());
				temporaryVisibilityMigration.migrateBucket(this.s3, sourceBucket, visibleBucket, fqn, Region.US_EAST_1);
			});
	}

	DefaultManagedFileService(JdbcClient db, Storage storage, ApplicationEventPublisher publisher,
			TransactionTemplate transactionTemplate,
			// todo delete the variable s3 it's only to support the migration!
			S3Client s3) {
		this.db = db;
		this.storage = storage;
		this.publisher = publisher;
		this.transactionTemplate = transactionTemplate;
		this.s3 = s3; // todo delete the variable s3 it's only to support the migration!
	}

	@Override
	public void write(Long managedFileId, String filename, MediaType mts, File resource) {
		this.write(managedFileId, filename, mts, new FileSystemResource(resource));
	}

	@Override
	public String getPublicUrlForManagedFile(Long managedFile) {
		// todo refactor this to work with cloudfront
		var mf = getManagedFile(managedFile);
		var url = "/public/managedfiles/" + mf.id();
		return (mf.visible()) ? url : null;
	}

	private ManagedFile forceReadManagedFile(Long managedFileId) {
		var managedFile = this.getManagedFile(managedFileId);
		this.managedFiles.get().remove(managedFileId);
		managedFile.contentType();// triggers the rehydration sideeffect. yuck.
		return managedFile;
	}

	/**
	 * meant to make sure we've synchronized the file write
	 */

	private void ensureVisibility(ManagedFile managedFile) {
		// todo should we do this on a thread? or launch it as a part of an
		// ApplicationEvent that runs asynchronously?
		// most of the time when we do writes, the writes will come from the http client,
		// so we'll only get one crack at the apple.
		// let's make sure its written to the main s3 bucket and then, if need be, well
		// copy it from that to this visible bucket.
		// do NOT read from the HTTP clients inputstream
		var visibleBucket = managedFile.visibleBucket();
		var folder = managedFile.folder();
		var fn = managedFile.storageFilename();
		var fqn = fqn(folder, fn);
		if (managedFile.visible()) {
			var resource = this.storage.read(managedFile.bucket(), fqn);
			this.storage.write(visibleBucket, fqn, resource);
		} //
		else {
			this.storage.remove(visibleBucket, fqn);
		}
	}

	@Override
	public void write(Long managedFileId, String filename, MediaType mediaType, Resource resource) {
		var managedFile = this.forceReadManagedFile(managedFileId);
		var bucket = managedFile.bucket();
		var folder = managedFile.folder();
		this.storage.write(bucket, fqn(folder, managedFile.storageFilename()), resource);
		this.ensureVisibility(managedFile);
		var clientMediaType = mediaType == null ? CommonMediaTypes.BINARY : mediaType;
		this.db.sql("update managed_file set filename =?, content_type =? , written = true , size =? where id=?")
			.params(filename, clientMediaType.toString(), contentLength(resource), managedFileId)
			.update();
		var freshManagedFile = this.forceReadManagedFile(managedFileId);
		this.transactionTemplate.execute(tx -> {
			this.publisher.publishEvent(new ManagedFileUpdatedEvent(freshManagedFile));
			return null;
		});
	}

	@Override
	public void setManagedFileVisibility(Long managedFile, boolean publicAccess) {
		this.db.sql("update managed_file set visible = ? where id = ?").params(publicAccess, managedFile).update();
		this.ensureVisibility(this.forceReadManagedFile(managedFile));
	}

	/**
	 * this one pulls down a {@link ManagedFile managed file}'s contents from S3, and then
	 * re-writes it, allowing us to synchronize our view of the S3 asset with the actual
	 * state of the S3 object.
	 */
	@Override
	@Transactional
	public void refreshManagedFile(Long managedFileId) {
		var managedFile = this.getManagedFile(managedFileId);
		var resource = this.read(managedFile.id());
		var tmp = FileUtils.tempFileWithExtension();
		try {
			try (var in = resource.getInputStream(); var out = new FileOutputStream(tmp)) {
				log.debug("starting download to local file [{}]", tmp.getAbsolutePath());
				FileCopyUtils.copy(in, out);
				log.debug("finished download to local file [{}]", tmp.getAbsolutePath());
			} //
			this.write(managedFile.id(), managedFile.filename(), CommonMediaTypes.MP3, tmp);
		} //
		catch (IOException e) {
			throw new RuntimeException(
					"could not refresh the file [" + tmp.getAbsolutePath() + "] for ManagedFile [" + managedFile + "]",
					e);
		} //
		finally {
			FileUtils.delete(tmp);
		}
		var mf = this.getManagedFile(managedFileId);
		log.debug("refreshed managed file {}", mf);
	}

	@Override
	public ManagedFileDeletionRequest getManagedFileDeletionRequest(Long managedFileDeletionRequestId) {
		return db.sql("select * from managed_file_deletion_request where id =? ")
			.param(managedFileDeletionRequestId)
			.query(new ManagedFileDeletionRequestRowMapper())
			.single();
	}

	@Override
	public Collection<ManagedFileDeletionRequest> getOutstandingManagedFileDeletionRequests() {
		return this.db//
			.sql("select * from managed_file_deletion_request where deleted = false")//
			.query(this.managedFileDeletionRequestRowMapper)//
			.list();
	}

	static String visibleBucketFor(String bucket) {
		return bucket + "-visible";
	}

	@Override
	public void completeManagedFileDeletion(Long managedFileDeletionRequestId) {
		var managedFileDeletionRequest = this.getManagedFileDeletionRequest(managedFileDeletionRequestId);
		Assert.notNull(managedFileDeletionRequest, "the managed file deletion request should not be null");
		var fqn = fqn(managedFileDeletionRequest.folder(), managedFileDeletionRequest.storageFilename());
		for (var bucket : new String[] { managedFileDeletionRequest.bucket(),
				managedFileDeletionRequest.visibleBucket() }) {
			this.storage.remove(bucket, fqn);
		}
		this.db.sql(" update managed_file_deletion_request set deleted = true where id = ? ")
			.param(managedFileDeletionRequestId)
			.update();
	}

	@Override
	@Transactional
	public void deleteManagedFile(Long managedFileId) {
		var managedFile = this.getManagedFile(managedFileId);
		// very important that we do this part FIRST since the calls to mogulId will
		// lazily trigger the loading of the managed_file, which will be deleted if we
		// waited until after the next line
		this.db.sql(
				"insert into managed_file_deletion_request ( mogul , bucket, folder, filename ,storage_filename) values(?,?,?,?,?)")
			.params(managedFile.mogulId(), //
					managedFile.bucket(), //
					managedFile.folder(), //
					managedFile.filename(), //
					managedFile.storageFilename() //
			)
			.update();
		this.db.sql("delete from managed_file where id =?").param(managedFileId).update();
		this.publisher.publishEvent(new ManagedFileDeletedEvent(managedFile));
	}

	private static String fqn(String folder, String filename) {
		return folder + '/' + filename;
	}

	@Override
	public Resource read(Long managedFileId) {
		// the call to getManagedFile needs to be in a transaction. the reading of bytes
		// does not.
		var mf = this.transactionTemplate.execute(status -> this.getManagedFile(managedFileId));
		var fn = fqn(mf.folder(), mf.storageFilename());
		var bucket = mf.visible() ? mf.visibleBucket() : mf.bucket();
		return this.storage.read(bucket, fn);
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
	public ManagedFile createManagedFile(Long mogulId, String bucket, String folder, String fileName, long size,
			MediaType mediaType, boolean visible) {
		var kh = new GeneratedKeyHolder();
		var sql = """
				insert into managed_file( storage_filename, mogul , bucket, folder, filename, size,content_type, visible)
				values (?,?,?,?,?,?,?,?)
				""";
		this.db.sql(sql)
			.params(UUID.randomUUID().toString(), mogulId, bucket, folder, fileName, size, mediaType.toString(),
					visible)
			.update(kh);
		return this.getManagedFile(((Number) Objects.requireNonNull(kh.getKeys()).get("id")).longValue());
	}

	private void initializeManagedFile(ResultSet rs, ManagedFile managedFile) throws SQLException {
		managedFile.hydrate(rs.getLong("mogul"), rs.getLong("id"), rs.getString("bucket"),
				rs.getString("storage_filename"), rs.getString("folder"), rs.getString("filename"),
				rs.getTimestamp("created"), rs.getBoolean("written"), rs.getLong("size"), rs.getString("content_type"),
				rs.getBoolean("visible"));
	}

	private final TransactionSynchronization transactionSynchronization = new TransactionSynchronization() {

		@Override
		public void beforeCompletion() {
			var managedFileMap = managedFiles.get();
			if (managedFileMap != null && !managedFileMap.isEmpty()) {
				log.trace("beforeCompletion(): for the current thread there are {} managed files ",
						managedFileMap.size());
				// let's get all the IDs, then visit each managed file and hydrate.
				var ids = managedFileMap.values()
					.stream()
					.map(ManagedFile::id)
					.map(i -> Long.toString(i))
					.collect(Collectors.joining(", "));

				db //
					.sql("select * from managed_file where id in ( " + ids + ")") //
					.query(rs -> {
						var mfId = rs.getLong("id");
						var managedFile = managedFileMap.get(mfId);
						initializeManagedFile(rs, managedFile);
					});

			} //

		}

	};

	/**
	 * returns lazy, stand-in proxies to {@link ManagedFile managedfiles } that
	 * materialize when used.
	 */
	@Override
	public ManagedFile getManagedFile(Long managedFileId) {

		return this.transactionTemplate.execute(tx -> {
			TransactionSynchronizationManager.registerSynchronization(this.transactionSynchronization);

			if (this.managedFiles.get() == null)
				this.managedFiles.set(new ConcurrentSkipListMap<>());

			// this allows any managed file that for whatever reason we're manipulating
			// and NOT able to wait for the transaction to commit to hydrate its state
			var hydration = (Consumer<ManagedFile>) managedFile -> this.db
				.sql("select * from managed_file where id = ?")
				.param(managedFileId)
				.query(rs -> {
					if (this.log.isTraceEnabled())
						this.log.trace("Manually hydrating ManagedFile #{}.".trim(), managedFileId);
					this.initializeManagedFile(rs, managedFile);
				});

			return this.managedFiles.get().computeIfAbsent(managedFileId, mid -> new ManagedFile(mid, hydration)); //

		});
	}

}
