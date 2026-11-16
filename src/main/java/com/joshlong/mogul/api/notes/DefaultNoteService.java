package com.joshlong.mogul.api.notes;

import com.joshlong.mogul.api.AbstractDomainService;
import com.joshlong.mogul.api.Notable;
import com.joshlong.mogul.api.NotableResolver;
import com.joshlong.mogul.api.Note;
import com.joshlong.mogul.api.SearchableDeletedEvent;
import com.joshlong.mogul.api.mogul.MogulService;
import com.joshlong.mogul.utils.JsonUtils;
import com.joshlong.mogul.utils.ReflectionUtils;
import com.joshlong.mogul.utils.UriUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.SqlArrayValue;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

import java.net.URI;
import java.util.*;

@SuppressWarnings("unchecked")
@Service
@Transactional
class DefaultNoteService extends AbstractDomainService<Notable, NotableResolver<?>> implements NoteService {

	private final ApplicationEventPublisher publisher;

	private final Logger log = LoggerFactory.getLogger(getClass());

	private final RowMapper<Note> noteRowMapper = (rs, _) -> {
		var done = rs.getTimestamp("done");
		return new Note(rs.getLong("mogul_id"), rs.getLong("id"), rs.getString("payload"),
				ReflectionUtils.classForName(rs.getString("payload_class")),
				new java.util.Date(rs.getTimestamp("created").getTime()), UriUtils.uri(rs.getString("url")),
				done == null ? null : new java.util.Date(done.getTime()), rs.getString("title"), rs.getString("note"));
	};

	private final JdbcClient db;

	private final MogulService mogulService;

	DefaultNoteService(Collection<NotableResolver<?>> resolvers, ApplicationEventPublisher publisher, JdbcClient db,
			MogulService mogulService) {
		super(resolvers);
		this.publisher = publisher;
		this.db = db;
		this.mogulService = mogulService;
	}

	@Override
	public <T extends Notable> T resolveNotable(Long mogulId, Long id, String clazz) {
		var type = (Class<? extends T>) this.classForType(clazz);
		return this.resolveNotable(mogulId, id, type);
	}

	@Override
	public boolean deleteNote(Long mogulId, Long noteId) {
		var deleted = this.db.sql("delete from note where id = ? and mogul_id = ?")
			.params(noteId, mogulId)
			.update() > 0;
		if (deleted)
			this.publisher.publishEvent(new SearchableDeletedEvent(Note.class, noteId));
		return deleted;
	}

	@Override
	public <T extends Notable> T resolveNotable(Long mogulId, Long id, Class<T> clazz) {
		this.mogulService.assertAuthorizedMogul(mogulId);
		var notable = this.findEntity(clazz, id);
		// a note may only go on the mogul's own things
		var owner = notable == null ? null : ((NotableResolver<T>) this.findResolver(clazz)).mogulId(notable);
		Assert.state(owner != null && owner.equals(mogulId),
				"there is no " + clazz.getSimpleName() + " [" + id + "] for mogul [" + mogulId + "]");
		return notable;
	}

	@Override
	public Note getNoteById(Long id) {
		return this.getNotesById(Collections.singleton(id)).get(id);
	}

	@Override
	public Map<Long, Note> getNotesById(Collection<Long> ids) {
		var l = this.db //
			.sql("select * from note where id = any( ? ) ") //
			.params(new SqlArrayValue("bigint", ids.toArray()))//
			.query(this.noteRowMapper) //
			.list();
		var m = new HashMap<Long, Note>();
		for (var i : l)
			m.put(i.id(), i);
		return m;
	}

	@Override
	public Collection<Note> getNotesByMogul(Long mogulId) {
		return this.db //
			.sql("select * from note where mogul_id = ? order by created") //
			.param(mogulId) //
			.query(this.noteRowMapper) //
			.list();
	}

	@Override
	public <T extends Notable> Collection<Note> notes(Long mogulId, Long id, String clazz) {
		return this.notes(mogulId, id, clazz, true);
	}

	@Override
	public <T extends Notable> Collection<Note> notes(Long mogulId, Long id, String clazz, boolean includeDone) {
		var payload = this.resolveNotable(mogulId, id, clazz);
		return this.notes(mogulId, payload, includeDone);
	}

	@Override
	public <T extends Notable> Collection<Note> notes(Long mogulId, T payload) {
		return this.notes(mogulId, payload, true);
	}

	@Override
	public <T extends Notable> Collection<Note> notes(Long mogulId, T payload, boolean includeDone) {
		var sql = """
				select *
				from note
				where mogul_id = ?
				  and payload = ?
				  and payload_class = ?
				""" + (includeDone ? "" : " and done is null ") + " order by created ";
		var list = this.db //
			.sql(sql)//
			.params(mogulId, JsonUtils.write(payload.notableKey()), payload.getClass().getName()) //
			.query(this.noteRowMapper)//
			.list();
		log.info("found {} notes for {} of type {}", list.size(), payload.notableKey(), payload.getClass().getName());
		return list;
	}

	@Override
	public Collection<Note> notesForUrl(Long mogulId, URI url) {
		if (url == null)
			return List.of();
		return this.db //
			.sql("""
					select *
					from note
					where mogul_id = ?
					  and url = ?
					order by created desc, id desc
					""") //
			.params(mogulId, url.toString()) //
			.query(this.noteRowMapper) //
			.list();
	}

	@Override
	public Note update(Long mogulId, Long noteId, URI url, String title, String note) {
		return this.update(mogulId, noteId, null, url, title, note);
	}

	@Override
	public <T extends Notable> Note update(Long mogulId, Long noteId, T notable, URI url, String title, String note) {
		NoteUrls.assertWebUrl(url);
		var updated = notable == null //
				? this.db.sql("update note set url = ?, title = ?, note = ? where id = ? and mogul_id = ?") //
					.params(url == null ? null : url.toString(), blankToNull(title), nonNull(note), noteId, mogulId) //
					.update() //
				: this.db.sql("""
						update note set url = ?, title = ?, note = ?, payload = ?, payload_class = ?
						where id = ? and mogul_id = ?
						""") //
					.params(url == null ? null : url.toString(), blankToNull(title), nonNull(note),
							JsonUtils.write(notable.notableKey()), notable.getClass().getName(), noteId, mogulId) //
					.update();
		if (updated == 0) {
			return null;
		}
		var noteUpdated = this.getNoteById(noteId);
		this.publisher.publishEvent(new NoteUpdatedEvent(noteUpdated));
		return noteUpdated;
	}

	@Override
	public <T extends Notable> Note create(Long mogulId, T payload, URI url, String title, String note) {
		NoteUrls.assertWebUrl(url);
		var payloadKeyAsJson = JsonUtils.write(payload.notableKey());
		var newNote = new Note(mogulId, null, payloadKeyAsJson, payload.getClass(), new Date(), url, null,
				blankToNull(title), nonNull(note));
		var kg = new GeneratedKeyHolder();
		this.db.sql("""
				 insert into note (mogul_id, created, payload, payload_class, url, title, note)
				 values (?, ?, ?, ?, ?, ?, ?)
				""")
			.params(mogulId, newNote.created(), newNote.payload(), newNote.payloadClass().getName(),
					url == null ? null : url.toString(), newNote.title(), newNote.note())
			.update(kg);

		var insertedId = ((Integer) Objects.requireNonNull(kg.getKeys()).get("id")).longValue();
		var n = this.getNoteById(insertedId);
		this.publisher.publishEvent(new NoteCreatedEvent(n));
		return n;
	}

	@Override
	public Note setDone(Long mogulId, Long noteId, boolean done) {
		var updated = this.db.sql("update note set done = ? where id = ? and mogul_id = ?")
			.params(done ? new Date() : null, noteId, mogulId)
			.update();
		if (updated == 0) {
			return null;
		}
		var noteUpdated = this.getNoteById(noteId);
		this.publisher.publishEvent(new NoteUpdatedEvent(noteUpdated));
		return noteUpdated;
	}

	private static String blankToNull(String s) {
		return s == null || s.isBlank() ? null : s.strip();
	}

	// a clip can be just a title and a url, but the column is not null
	private static String nonNull(String s) {
		return s == null ? "" : s;
	}

}
