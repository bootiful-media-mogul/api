package com.joshlong.mogul.api.notes;

import com.joshlong.mogul.api.Notable;
import com.joshlong.mogul.api.Note;

import org.springframework.util.DigestUtils;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Map;

public interface NoteService {

	<T extends Notable> T resolveNotable(Long mogulId, Long id, String clazz);

	boolean deleteNote(Long mogulId, Long noteId);

	<T extends Notable> T resolveNotable(Long mogulId, Long id, Class<T> clazz);

	Note getNoteById(Long id);

	Map<Long, Note> getNotesById(Collection<Long> ids);

	Collection<Note> getNotesByMogul(Long mogulId);

	<T extends Notable> Collection<Note> notes(Long mogulId, Long id, String clazz);

	<T extends Notable> Collection<Note> notes(Long mogulId, Long id, String clazz, boolean includeDone);

	<T extends Notable> Collection<Note> notes(Long mogulId, T payload);

	<T extends Notable> Collection<Note> notes(Long mogulId, T payload, boolean includeDone);

	/**
	 * the given mogul's notes that point to exactly this url, wherever they're attached,
	 * newest first. it's how the clipper finds the note it already made for a page.
	 */
	Collection<Note> notesForUrl(Long mogulId, URI url);

	<T extends Notable> Note create(Long mogulId, T payload, URI url, String title, String note);

	/**
	 * replaces the title, text, and url of a note that belongs to the given mogul.
	 * @return the updated note, or {@code null} if the mogul has no such note
	 */
	Note update(Long mogulId, Long noteId, URI url, String title, String note);

	/**
	 * as {@link #update(Long, Long, URI, String, String)}, and moves the note to the
	 * given notable, unless it's {@code null}. resolve the notable with
	 * {@link #resolveNotable(Long, Long, String)}, which makes sure the mogul owns it.
	 */
	<T extends Notable> Note update(Long mogulId, Long noteId, T notable, URI url, String title, String note);

	Note setDone(Long mogulId, Long noteId, boolean done);

	/**
	 * a summary of the page at a note's url: it becomes the note's text if the note has
	 * none, and goes under the text if it has some. it's only written if the note's text
	 * is still what it was when the summary was asked for -- see
	 * {@link #textHash(String)} -- so a summary can't be written twice, or land on an
	 * edit it never saw.
	 * @return the updated note, or {@code null} if the mogul has no such note, or its
	 * text has changed
	 */
	Note appendSummary(Long mogulId, Long noteId, String textHash, String summary);

	/**
	 * gives a note with no title this one. a note that has a title -- the clipper's, or
	 * one somebody typed -- keeps it.
	 * @return the updated note, or {@code null} if the mogul has no such note, or it
	 * already has a title
	 */
	Note fillTitle(Long mogulId, Long noteId, String title);

	/**
	 * the hash of a note's text that {@link #appendSummary(Long, Long, String, String)}
	 * compares. it's the same as PostgreSQL's {@code md5(note)}.
	 */
	static String textHash(String note) {
		return DigestUtils.md5DigestAsHex((note == null ? "" : note).getBytes(StandardCharsets.UTF_8));
	}

}
