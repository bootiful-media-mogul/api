package com.joshlong.mogul.api.notes;

import com.joshlong.mogul.api.Note;
import com.joshlong.mogul.api.mogul.MogulService;
import com.joshlong.mogul.utils.DateUtils;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.graphql.data.ArgumentValue;
import org.springframework.graphql.data.method.annotation.Argument;
import org.springframework.graphql.data.method.annotation.MutationMapping;
import org.springframework.graphql.data.method.annotation.QueryMapping;
import org.springframework.stereotype.Controller;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.Collection;

@Controller
class NotesController {

	private final MogulService mogulService;

	private final NoteService noteService;

	private final ApplicationEventPublisher publisher;

	NotesController(MogulService mogulService, NoteService noteService, ApplicationEventPublisher publisher) {
		this.mogulService = mogulService;
		this.noteService = noteService;
		this.publisher = publisher;
	}

	@MutationMapping
	boolean deleteNote(@Argument Long id) {
		var currentMogul = this.mogulService.getCurrentMogul();
		return this.noteService.deleteNote(currentMogul.id(), id);
	}

	@MutationMapping
	boolean updateNote(@Argument Long id, @Argument String note, ArgumentValue<String> url) {
		var mogul = this.mogulService.getCurrentMogul();
		var newUrl = (URI) null;
		if (url.isOmitted()) {
			var existing = this.noteService.getNoteById(id);
			if (existing == null || !existing.mogulId().equals(mogul.id()))
				return false;
			newUrl = existing.url();
		} //
		else {
			newUrl = NoteUrls.parse(url.value());
		}
		return this.noteService.update(mogul.id(), id, newUrl, note) != null;
	}

	@MutationMapping
	boolean setNoteDone(@Argument Long id, @Argument Boolean done) {
		var currentMogul = this.mogulService.getCurrentMogul();
		return this.noteService.setDone(currentMogul.id(), id, Boolean.TRUE.equals(done)) != null;
	}

	@MutationMapping
	boolean createNote(@Argument String type, @Argument Long id, @Argument String note, @Argument String url) {
		var mogul = this.mogulService.getCurrentMogul();
		var parsedUrl = NoteUrls.parse(url);
		var payload = this.noteService.resolveNotable(mogul.id(), id, type);
		var newNote = this.noteService.create(mogul.id(), payload, parsedUrl, note);

		return newNote != null;
	}

	@QueryMapping
	Collection<ClientNote> notesForNotable(@Argument String type, @Argument Long id, @Argument Boolean includeDone) {
		var currentMogul = this.mogulService.getCurrentMogul();
		return this.noteService //
			.notes(currentMogul.id(), id, type, Boolean.TRUE.equals(includeDone))//
			.stream()//
			.map(note -> this.note(type, note))//
			.toList();
	}

	private ClientNote note(String type, Note note) {
		return new ClientNote(type, note.id(), DateUtils.forDate(note.created()), DateUtils.forDate(note.done()),
				note.url(), note.note());
	}

}

record ClientNote(String type, Long id, OffsetDateTime created, OffsetDateTime done, URI url, String note) {
}

record NoteCreatedEvent(Note note) {
}

record NoteUpdatedEvent(Note note) {
}
