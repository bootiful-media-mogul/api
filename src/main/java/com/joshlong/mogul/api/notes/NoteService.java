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
	 * the given mogul's notes on the given notable that point to exactly this url, newest
	 * first. it's how the clipper finds the note it already made for a page.
	 */
	<T extends Notable> Collection<Note> notesForUrl(Long mogulId, T payload, URI url);

	<T extends Notable> Note create(Long mogulId, T payload, URI url, String title, String note);

	/**
	 * replaces the title, text, and url of a note that belongs to the given mogul.
	 * @return the updated note, or {@code null} if the mogul has no such note
	 */
	Note update(Long mogulId, Long noteId, URI url, String title, String note);

	Note setDone(Long mogulId, Long noteId, boolean done);

}
