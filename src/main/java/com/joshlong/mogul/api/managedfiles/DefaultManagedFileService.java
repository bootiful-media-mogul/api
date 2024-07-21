package com.joshlong.mogul.api.managedfiles;

import com.joshlong.mogul.api.utils.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;
import org.springframework.util.FileCopyUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Objects;
import java.util.UUID;

@Service
@Transactional
class DefaultManagedFileService implements ManagedFileService {

	private final JdbcClient db;

	private final Storage storage;

	private final Logger log = LoggerFactory.getLogger(getClass());

	private final ApplicationEventPublisher publisher;

	DefaultManagedFileService(JdbcClient db, Storage storage, ApplicationEventPublisher publisher) {
		this.db = db;
		this.storage = storage;
		this.publisher = publisher;
	}

	@Override
	public void write(Long managedFileId, String filename, MediaType mts, File resource) {
		this.write(managedFileId, filename, mts, new FileSystemResource(resource));
	}

	@Override
	public void write(Long managedFileId, String filename, MediaType mediaType, Resource resource) {
		var managedFile = getManagedFile(managedFileId);
		var bucket = managedFile.bucket();
		var folder = managedFile.folder();
		this.storage.write(bucket, folder + '/' + managedFile.storageFilename(), resource);
		var clientMediaType = mediaType == null ? CommonMediaTypes.BINARY : mediaType;
		this.db.sql("update managed_file set filename =?, content_type =? , written = true , size =? where id=?")
			.params(filename, clientMediaType.toString(), contentLength(resource), managedFileId)
			.update();
		log.debug("are we uploading on a virtual thread? {}", Thread.currentThread().isVirtual());
		var freshManagedFile = getManagedFile(managedFileId);
		log.debug("managed file has been written? {}", freshManagedFile.written());
		this.publisher.publishEvent(new ManagedFileUpdatedEvent(freshManagedFile));
	}

	/**
	 * this one pulls down a {@link ManagedFile managed file}'s contents from S3, and then
	 * re-writes it, allowing us to synchronize our view of the S3 asset with the actual
	 * state of the S3 object.
	 */
	@Override
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
		return this.db.sql("select * from managed_file_deletion_request where deleted = false")
			.query(new ManagedFileDeletionRequestRowMapper())
			.list();
	}

	@Override
	public void completeManagedFileDeletion(Long managedFileDeletionRequestId) {
		var mfRequest = getManagedFileDeletionRequest(managedFileDeletionRequestId);
		Assert.notNull(mfRequest, "the managed file deletion request should not be null");
		storage.remove(mfRequest.bucket(), mfRequest.folder() + '/' + mfRequest.storageFilename());
		this.db.sql(" update  managed_file_deletion_request  set deleted = true where id = ? ")
			.param(managedFileDeletionRequestId)
			.update();
		var managedFileDeletionRequest = getManagedFileDeletionRequest(managedFileDeletionRequestId);
		log.debug("completed [{}]", managedFileDeletionRequest);
	}

	@Override
	public void deleteManagedFile(Long managedFileId) {
		var mf = getManagedFile(managedFileId);
		db.sql("delete from managed_file where id =?").param(managedFileId).update();
		db.sql("insert into managed_file_deletion_request ( mogul_id, bucket, folder, filename ,storage_filename) values(?,?,? ,?,?)")
			.params(mf.mogulId(), mf.bucket(), mf.folder(), mf.filename(), mf.storageFilename())
			.update();
	}

	@Override
	public ManagedFile getManagedFile(Long managedFileId) {
		if (null == managedFileId || managedFileId == 0)
			return null;
		return this.db.sql("select * from managed_file where id =? ")
			.param(managedFileId)
			.query(new ManagedFileRowMapper())
			.single();
	}

	@Override
	public Resource read(Long managedFileId) {
		var mf = getManagedFile(managedFileId);
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
	public ManagedFile createManagedFile(Long mogulId, String bucket, String folder, String fileName, long size,
			MediaType mediaType) {
		var kh = new GeneratedKeyHolder();
		this.db.sql("""
					insert into managed_file( storage_filename, mogul_id, bucket, folder, filename, size,content_type)
					VALUES (?,?,?,?,?,?,?)
				""")
			.params(UUID.randomUUID().toString(), mogulId, bucket, folder, fileName, size, mediaType.toString())
			.update(kh);
		return getManagedFile(((Number) Objects.requireNonNull(kh.getKeys()).get("id")).longValue());
	}

}

class ManagedFileDeletionRequestRowMapper implements RowMapper<ManagedFileDeletionRequest> {

	@Override
	public ManagedFileDeletionRequest mapRow(ResultSet rs, int rowNum) throws SQLException {
		return new ManagedFileDeletionRequest(rs.getLong("id"), rs.getLong("mogul_id"), rs.getString("bucket"),
				rs.getString("folder"), rs.getString("filename"), rs.getString("storage_filename"),
				rs.getBoolean("deleted"), rs.getDate("created"));
	}

}

class ManagedFileRowMapper implements RowMapper<ManagedFile> {

	@Override
	public ManagedFile mapRow(ResultSet rs, int rowNum) throws SQLException {
		return new ManagedFile(rs.getLong("mogul_id"), rs.getLong("id"), rs.getString("bucket"),
				rs.getString("storage_filename"), rs.getString("folder"), rs.getString("filename"),
				rs.getDate("created"), rs.getBoolean("written"), rs.getLong("size"), rs.getString("content_type"));
	}

}