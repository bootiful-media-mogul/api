package com.joshlong.mogul.api.notes.search;

import com.joshlong.mogul.api.AbstractSearchableResolver;
import com.joshlong.mogul.api.Note;
import com.joshlong.mogul.api.SearchableResult;
import com.joshlong.mogul.api.notes.NoteService;
import com.joshlong.mogul.utils.TypeUtils;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
class NoteSearchableResolver extends AbstractSearchableResolver<Note> {

	private final NoteService noteService;

	NoteSearchableResolver(NoteService noteService) {
		super(Note.class);
		this.noteService = noteService;
	}

	@Override
	public List<SearchableResult<Note>> results(List<Long> searchableIds) {
		if (searchableIds == null || searchableIds.isEmpty())
			return Collections.emptyList();
		var notes = this.noteService.getNotesById(searchableIds);
		var searchableResults = new ArrayList<SearchableResult<Note>>(notes.size());
		for (var note : notes.values()) {
			searchableResults.add(this.searchableNote(note));
		}
		return searchableResults;
	}

	private SearchableResult<Note> searchableNote(Note note) {
		var notableId = Long.parseLong(note.payload());
		var context = Map.of("type", (Object) TypeUtils.typeName(Note.class), "id", note.id());
		var title = note.title() != null ? note.title() : note.note();
		var text = note.url() == null ? note.note() : note.note() + "\n" + note.url();
		return new SearchableResult<>(note.searchableId(), note, title, text, notableId, new Date(), this.type,
				context);
	}

	@Override
	public Note find(Long key) {
		return this.noteService.getNoteById(key);
	}

}
