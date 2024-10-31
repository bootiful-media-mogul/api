package com.joshlong.mogul.api.managedfiles;

import com.joshlong.mogul.api.utils.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.Assert;
import org.springframework.util.FileCopyUtils;

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
@Service
class DefaultManagedFileService implements ManagedFileService {

	private final Logger log = LoggerFactory.getLogger(getClass());

	private final ManagedFileDeletionRequestRowMapper managedFileDeletionRequestRowMapper = new ManagedFileDeletionRequestRowMapper();

	private final ThreadLocal<Map<Long, ManagedFile>> managedFiles = new ThreadLocal<>();

	private final JdbcClient db;

	private final Storage storage;

	private final ApplicationEventPublisher publisher;

	private final TransactionTemplate transactionTemplate;

	DefaultManagedFileService(JdbcClient db, Storage storage, ApplicationEventPublisher publisher,
			TransactionTemplate transactionTemplate) {
		this.db = db;
		this.storage = storage;
		this.publisher = publisher;
		this.transactionTemplate = transactionTemplate;
	}

	@Override
	public void write(Long managedFileId, String filename, MediaType mts, File resource) {
		this.write(managedFileId, filename, mts, new FileSystemResource(resource));
	}

	private ManagedFile forceReadManagedFile(Long managedFileId) {
		var managedFile = this.getManagedFile(managedFileId);
		this.managedFiles.get().remove(managedFileId);
		managedFile.contentType();// triggers the rehydration sideeffect. yuck.
		return managedFile;
	}

	@Override
	public void write(Long managedFileId, String filename, MediaType mediaType, Resource resource) {
		var managedFile = this.forceReadManagedFile(managedFileId);
		var bucket = managedFile.bucket();
		var folder = managedFile.folder();
		this.storage.write(bucket, folder + '/' + managedFile.storageFilename(), resource);
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
			try (var in = (resource.getInputStream()); var out = (new FileOutputStream(tmp))) {
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

	@Override
	public void completeManagedFileDeletion(Long managedFileDeletionRequestId) {
		var managedFileDeletionRequest = this.getManagedFileDeletionRequest(managedFileDeletionRequestId);
		Assert.notNull(managedFileDeletionRequest, "the managed file deletion request should not be null");
		storage.remove(managedFileDeletionRequest.bucket(),
				managedFileDeletionRequest.folder() + '/' + managedFileDeletionRequest.storageFilename());
		this.db.sql(" update  managed_file_deletion_request  set deleted = true where id = ? ")
			.param(managedFileDeletionRequestId)
			.update();
	}

	@Override
	@Transactional
	public void deleteManagedFile(Long managedFileId) {
		var managedFile = this.getManagedFile(managedFileId);
		// very important that we do this part FIRST since the calls to mogulId will
		// lazily trigger the loading of the managed_file, which will be deleted if we
		// waited unti after the next line
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

	@Override
	public Resource read(Long managedFileId) {
		// the call to getManagedFile needs to be in a transaction. the reading of bytes
		// most assuredly does not.
		var mf = this.transactionTemplate.execute(status -> this.getManagedFile(managedFileId));
		return this.storage.read(mf.bucket(), mf.folder() + '/' + mf.storageFilename());
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
