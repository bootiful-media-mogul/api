package com.joshlong.mogul.api.managedfiles;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.joshlong.mogul.api.utils.FileUtils;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.io.File;
import java.net.URI;
import java.util.Date;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * represents a persistent, managed file stored on cloud storage
 */

public class ManagedFile {

	private final AtomicBoolean visible = new AtomicBoolean(false);

	private final AtomicReference<Long> mogulId = new AtomicReference<>();

	private final AtomicReference<Long> id = new AtomicReference<>();

	private final AtomicReference<String> bucket = new AtomicReference<>();

	private final AtomicReference<String> storageFilename = new AtomicReference<>();

	private final AtomicReference<String> folder = new AtomicReference<>();

	private final AtomicReference<String> filename = new AtomicReference<>();

	private final AtomicReference<Date> created = new AtomicReference<>();

	private final AtomicReference<Boolean> written = new AtomicReference<>();

	private final AtomicReference<Long> size = new AtomicReference<>();

	private final AtomicReference<String> contentType = new AtomicReference<>();

	private final AtomicBoolean initialized = new AtomicBoolean();

	private final Consumer<ManagedFile> hydration;

	ManagedFile(Long managedFileId, Consumer<ManagedFile> hydration) {
		this.id.set(managedFileId);
		this.initialized.set(false);
		this.hydration = hydration;
		Assert.notNull(this.hydration, "the hydration function should not be null");
	}

	// private implementation detail
	void hydrate(Long mogulId, Long id, String bucket, String storageFilename, String folder, String filename,
			Date created, boolean written, long size, String contentType, boolean visible) {
		this.mogulId.set(mogulId);
		this.id.set(id);
		this.bucket.set(bucket);
		this.storageFilename.set(storageFilename);
		this.folder.set(folder);
		this.filename.set(filename);
		this.created.set(created);
		this.written.set(written);
		this.size.set(size);
		this.contentType.set(contentType);
		this.visible.set(visible);
		// very important
		this.initialized.set(true);
	}

	private void ensureInitialized() {
		if (!this.initialized.get()) {
			// todo should this failure result in a read through to the DB? is there some
			// way to make this lazily load its own data? can we give it a callback?
			this.hydration.accept(this);
			Assert.state(this.initialized.get(),
					"managed file #" + this.id() + " should be initialized by this point.");
		}
	}

	@JsonProperty("mogulId")
	public Long mogulId() {
		this.ensureInitialized();
		return mogulId.get();
	}

	@JsonProperty("id")
	public Long id() {
		return id.get();
	}

	@JsonProperty("bucket")
	public String bucket() {
		this.ensureInitialized();
		return bucket.get();
	}

	@JsonProperty("storageFilename")
	public String storageFilename() {
		this.ensureInitialized();
		return storageFilename.get();
	}

	@JsonProperty("folder")
	public String folder() {
		this.ensureInitialized();
		return this.folder.get();
	}

	@JsonProperty("filename")
	public String filename() {
		this.ensureInitialized();
		return this.filename.get();
	}

	@JsonProperty("created")
	public Date created() {
		this.ensureInitialized();
		return this.created.get();
	}

	@JsonProperty("visible")
	public boolean visible() {
		this.ensureInitialized();
		return this.visible.get();
	}

	@JsonProperty("written")
	public boolean written() {
		this.ensureInitialized();
		return this.written.get();
	}

	@JsonProperty("size")
	public long size() {
		this.ensureInitialized();
		return this.size.get();
	}

	@JsonProperty("contentType")
	public String contentType() {
		this.ensureInitialized();
		return this.contentType.get();
	}

	@Override
	public String toString() {
		return "ManagedFile{" + "mogulId=" + mogulId.get() + ", id=" + id.get() + ", bucket='" + bucket.get() + '\''
				+ ", storageFilename='" + storageFilename.get() + +'\'' + ", folder='" + folder.get() + '\''
				+ ", filename='" + filename.get() + '\'' + ", created=" + created.get() + ", written=" + written.get()
				+ "," + ", visible=" + visible.get() + "," + " size=" + size.get() + ", contentType='"
				+ contentType.get() + '\'' + '}';
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;
		var that = (ManagedFile) o;
		return that.id().equals(this.id());
	}

	@Override
	public int hashCode() {
		return Objects.hash(this.id());// mogulId, id, bucket, storageFilename, folder,
										// filename, created, written, size, contentType);
	}

	public File uniqueLocalFile() {
		var extension = "";
		var periodIndex = filename().lastIndexOf('.');
		if (periodIndex != -1) {
			extension = filename().substring(periodIndex);
			if (StringUtils.hasText(extension) && extension.startsWith("."))
				extension = extension.substring(1);
		}
		return FileUtils.tempFile("managed-files-" + id, extension);
	}

	public URI s3Uri() {
		return URI.create("s3://" + this.bucket() + "/" + this.folder() + '/' + this.storageFilename());
	}

}
