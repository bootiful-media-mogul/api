package com.joshlong.mogul.api.compositions;

import com.joshlong.mogul.api.compositions.attachments.previews.MarkdownPreview;
import com.joshlong.mogul.api.managedfiles.CommonMediaTypes;
import com.joshlong.mogul.api.managedfiles.ManagedFile;
import com.joshlong.mogul.api.managedfiles.ManagedFileService;
import com.joshlong.mogul.api.managedfiles.ManagedFileUpdatedEvent;
import com.joshlong.mogul.api.notifications.NotificationEvent;
import com.joshlong.mogul.api.notifications.NotificationEvents;
import com.joshlong.mogul.utils.CacheUtils;
import com.joshlong.mogul.utils.CollectionUtils;
import com.joshlong.mogul.utils.JdbcUtils;
import com.joshlong.mogul.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.Cache;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.SqlArrayValue;
import org.springframework.modulith.events.ApplicationModuleListener;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

import java.util.*;
//todo the attachments pointing to a ManagedFile thats been cached with the wrong content-type.
// the content-type is determined asynchronously, but the CompositionService should be able to invalidate it.

/**
 * helps to manage the lifecycle and entities associated with a given composition, which
 * are blocks of text with attachments. for now, we'll assume attachments are images.
 * maybe one day we'll be able to embed, somehow, audio and videos that have been dragged
 * into a text block. it wouldn't be so hard. some sort of strategy that - given a
 * particular {@link ManagedFile}, consults the {@link ManagedFile#contentType()} and
 * helps to render Markdown that embeds a {@code <video>} player or a {@code <audio>}
 * player or an {@code <img >} tag, as appropriate. for now, though. images.
 */
@Transactional
class DefaultCompositionService implements CompositionService {

	private final Logger log = LoggerFactory.getLogger(getClass());

	private final JdbcClient db;

	private final ManagedFileService managedFileService;

	private final AttachmentResultSetExtractor attachmentResultSetExtractor;

	private final Cache compositionsByKeyCache, attachmentsCache, compositionsByIdCache;

	private final ResultSetExtractor<Collection<Composition>> compositionResultSetExtractor;

	private final MarkdownPreview[] markdownPreviews;

	DefaultCompositionService(JdbcClient db, Cache compositionsByKeyCache, Cache compositionsByIdCache,
			Cache attachmentsCache, ManagedFileService managedFileService, MarkdownPreview[] markdownPreviews) {
		this.db = db;
		this.compositionsByIdCache = compositionsByIdCache;
		this.attachmentsCache = attachmentsCache;
		this.compositionsByKeyCache = compositionsByKeyCache;
		this.managedFileService = managedFileService;
		this.attachmentResultSetExtractor = new AttachmentResultSetExtractor(this.managedFileService::getManagedFiles);
		this.compositionResultSetExtractor = new CompositionResultSetExtractor(this.db,
				this.attachmentResultSetExtractor);
		this.markdownPreviews = markdownPreviews;
	}

	@ApplicationModuleListener
	void onManagedFileEvent(ManagedFileUpdatedEvent event) {

		record CompositionAndAttachment(Long compositionId, Long attachmentId) {
		}

		this.log.debug("in {}, received a ManagedFileUpdatedEvent for {}", this.getClass().getSimpleName(),
				event.managedFile());
		var managedFileEventId = event.managedFile().id();
		// only the two ids are read below, so this deliberately does not resolve the
		// attachments' managed files: doing so cost a lookup per row for an object that
		// was thrown away, and the one managed file in question is already in hand on
		// the event itself.
		var attachments = db //
			.sql("select composition_id, id from composition_attachment where managed_file_id = ?")//
			.param(managedFileEventId) //
			.query((rs, _) -> new CompositionAndAttachment(rs.getLong("composition_id"), rs.getLong("id"))) //
			.list();
		if (attachments.isEmpty())
			return;
		else
			this.log.info("there are {} attachments for managed file {}", attachments.size(), managedFileEventId);

		var compositions = new HashSet<Long>();
		for (var meta : attachments) {
			this.invalidateAttachmentCache(meta.attachmentId());
			compositions.add(meta.compositionId());
		}
		if (!compositions.isEmpty())
			this.invalidateCompositionCacheById(compositions.iterator().next());

		NotificationEvents.notifyAsync(NotificationEvent.systemNotificationEventFor(event.managedFile().mogulId(),
				new AttachmentManagedFileUpdatedEvent(managedFileEventId), Long.toString(event.managedFile().id()),
				null));
	}

	@Override
	public Composition getCompositionById(Long id) {
		return this.getCompositionsByIds(Collections.singleton(id)).get(id);
	}

	private Composition readThroughCompositionById(Long id) {
		if (this.compositionsByIdCache.get(id) == null) {
			var comps = this.db //
				.sql("select * from composition where id = ?")//
				.param(id)//
				.query(this.compositionResultSetExtractor);
			var c = CollectionUtils.firstOrNull(comps);
			doCache(id, c);
		}
		return this.compositionsByIdCache.get(id, Composition.class);
	}

	private void doCache(Long id, Composition composition) {
		this.compositionsByIdCache.put(id, composition);
		if (composition != null)
			this.compositionsByKeyCache.put(compositionKey(composition), composition);
	}

	@Override
	public Map<Long, Composition> getCompositionsByIds(Collection<Long> ids) {
		if (ids == null || ids.isEmpty()) {
			return new HashMap<>();
		}
		var map = new HashMap<Long, Composition>();
		// only the ids we haven't already got go to the database. reading the whole
		// batch unconditionally would trade one rare cold query for a guaranteed one
		// on every call -- and the cache is warm nearly all the time.
		var idsNotInCache = CacheUtils.notPresentInCache(this.compositionsByIdCache, ids);
		if (!idsNotInCache.isEmpty()) {
			var allComps = this.db //
				.sql("select * from composition where id = ANY(?)") //
				.param(new SqlArrayValue("bigint", idsNotInCache.toArray())) //
				.query(this.compositionResultSetExtractor);
			for (var composition : allComps) {
				var id = composition.id();
				map.put(id, composition);
				this.doCache(id, composition);
			}
		}
		for (var id : ids) {
			var cached = this.compositionsByIdCache.get(id, Composition.class);
			if (cached != null)
				map.put(id, cached);
		}
		return map;
	}

	@Override
	public void deleteCompositionById(Long id) {
		var composition = this.readThroughCompositionById(id);
		if (composition == null) {
			return;
		}

		if (composition.attachments() != null && !composition.attachments().isEmpty())
			composition.attachments().forEach(attachment -> {
				this.deleteCompositionAttachmentyId(attachment.id());
			});
		this.db.sql("delete from composition where id = ?").param(id).update();
	}

	@Override
	public Composition getCompositionByAttachmentId(Long attachmentId) {
		return this.db //
			.sql("select composition_id from composition_attachment where id = ?") //
			.param(attachmentId) //
			.query(Long.class) //
			.optional() //
			.map(this::getCompositionById) //
			.orElse(null);
	}

	@Override
	public void deleteCompositionAttachmentyId(Long id) {
		// this is gross, but i need the composition_id for the caches
		var compositionId = this.db //
			.sql("select composition_id from composition_attachment where id = ?") //
			.param(id) //
			.query((rs, _) -> rs.getLong("composition_id"))//
			.single();
		var composition = this.readThroughCompositionById(compositionId);
		var attachmentById = this.readThroughAttachmentById(id);
		var mf = attachmentById.managedFile();
		this.db.sql("delete from composition_attachment where id = ?").params(id).update();
		this.invalidateAttachmentCache(id);
		this.managedFileService.deleteManagedFile(mf.id());
		this.invalidateCompositionCacheById(composition.id());
		this.invalidateCompositionCacheByKey(composition);
	}

	@Override
	public String createMarkdownPreviewForAttachment(Attachment attachment) {
		this.log.debug("creating a markdown preview for attachment {} with content-type {}", attachment,
				attachment.managedFile().contentType());
		for (var candidate : this.markdownPreviews) {
			if (candidate.supports(attachment)) {
				return candidate.preview(attachment);
			}
		}
		return null;
	}

	private Attachment readThroughAttachmentById(Long id) {
		var attachment = this.attachmentsCache.get(id, Attachment.class);
		if (attachment == null) {
			// one row, but read through the same extractor as every other attachment, so
			// there is exactly one place that knows how to turn these rows into objects.
			var attachments = this.db //
				.sql("select * from composition_attachment where id = ?") //
				.param(id) //
				.query(this.attachmentResultSetExtractor) //
				.values()
				.stream()
				.flatMap(Collection::stream)
				.toList();
			Assert.state(attachments.size() == 1, "there should be exactly one attachment for the given id " + id
					+ " but there were " + attachments.size() + " instead");
			for (var a : attachments) {
				this.attachmentsCache.put(id, a);
				attachment = a;
			}
		}
		return attachment;
	}

	private void invalidateCompositionCacheByKey(Composition composition) {
		var key = compositionKey(composition);
		this.compositionsByKeyCache.evictIfPresent(key);
	}

	private Composition readThroughCompositionByKey(Class<?> clzz, String key, String field) {
		var compositionCacheKey = this.compositionKey(clzz, key, field);
		var clazzName = clzz.getName();
		return this.compositionsByKeyCache.get(compositionCacheKey, () -> {
			// read before write. going straight to the insert meant every cold read of
			// a composition that already existed -- which is nearly all of them, since
			// a composition is created once and read forever -- wrote to the table and
			// its write-ahead log only to discover it had nothing to do.
			var existing = this.selectCompositionByKey(clazzName, key, field);
			if (existing != null)
				return existing;
			this.db //
				.sql("""
						    insert into composition( payload_class,payload, field) values (?,?,?)
						    on conflict on constraint composition_payload_class_payload_field_key
						    do nothing
						""")//
				.params(clazzName, key, field) //
				.update();
			return this.selectCompositionByKey(clazzName, key, field);
		});
	}

	private Composition selectCompositionByKey(String clazzName, String key, String field) {
		return CollectionUtils.firstOrNull(this.db //
			.sql("select * from composition where payload_class = ? and payload = ?  and field = ? ")//
			.params(clazzName, key, field)//
			.query(this.compositionResultSetExtractor));
	}

	private void invalidateCompositionCacheById(Long compositionId) {
		this.compositionsByIdCache.evictIfPresent(compositionId);
	}

	private void invalidateAttachmentCache(Long attachmentId) {
		this.attachmentsCache.evictIfPresent(attachmentId);
	}

	private String compositionKey(Class<?> payloadClass, String compositionKeyAsJson, String field) {
		return payloadClass.getName() + ":" + compositionKeyAsJson + ":" + field;
	}

	private String compositionKey(Composition composition) {
		return this.compositionKey(composition.payloadClass(), composition.payload(), composition.field());
	}

	@Override
	public Composition compose(Class<? extends Composable> payloadClass, Long compositionKey, String field) {
		// readThroughCompositionByKey already creates the row when it isn't there, so
		// there is nothing here to retry. the fallback this replaces re-ran the same
		// on-conflict-do-nothing insert -- a guaranteed no-op, since the read-through
		// had just run it -- and then re-read a cache key it had itself memoized as
		// absent, so it could only ever return the same null it was trying to recover
		// from. two statements, one of them a write, to arrive back where it started.
		return this.readThroughCompositionByKey(payloadClass, JsonUtils.write(compositionKey), field);
	}

	@Override
	public Attachment createCompositionAttachment(Long mogul, Long compositionId, String key) {
		var managedFile = this.managedFileService.createManagedFile(mogul, "compositions", "", 0,
				CommonMediaTypes.BINARY, true);
		var gkh = new GeneratedKeyHolder();
		this.db.sql("""
				insert into composition_attachment( caption, composition_id, managed_file_id )
				values (?,?,?)
				""")//
			.params(key, compositionId, managedFile.id())//
			.update(gkh);
		var newAttachmentId = JdbcUtils.getIdFromKeyHolder(gkh).longValue();
		this.invalidateCompositionCacheById(compositionId);
		this.invalidateCompositionCacheByKey(this.readThroughCompositionById(compositionId));
		return this.readThroughAttachmentById(newAttachmentId);
	}

	public record AttachmentManagedFileUpdatedEvent(long managedFileId) {
	}

}
