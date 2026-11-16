package com.joshlong.mogul.api.notes;

import com.joshlong.mogul.api.Notable;
import com.joshlong.mogul.api.Note;

import java.net.URI;
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

}
