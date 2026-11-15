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

	// the type name of a note on the mogul itself, as the clients know it
	private static final String MOGUL_TYPE = "mogul";

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
	boolean updateNote(@Argument Long id, @Argument String note, ArgumentValue<String> url,
			ArgumentValue<String> title) {
		var mogul = this.mogulService.getCurrentMogul();
		// leaving url or title out keeps what the note has: the studio edits only the
		// text, and must not wipe what the clipper saved
		var existing = (url.isOmitted() || title.isOmitted()) ? this.noteService.getNoteById(id) : null;
		if ((url.isOmitted() || title.isOmitted()) && (existing == null || !existing.mogulId().equals(mogul.id())))
			return false;
		var newUrl = url.isOmitted() ? existing.url() : NoteUrls.parse(url.value());
		var newTitle = title.isOmitted() ? existing.title() : title.value();
		return this.noteService.update(mogul.id(), id, newUrl, newTitle, note) != null;
	}

	@MutationMapping
	boolean setNoteDone(@Argument Long id, @Argument Boolean done) {
		var currentMogul = this.mogulService.getCurrentMogul();
		return this.noteService.setDone(currentMogul.id(), id, Boolean.TRUE.equals(done)) != null;
	}

	@MutationMapping
	boolean createNote(@Argument String type, @Argument Long id, @Argument String note, @Argument String url,
			@Argument String title) {
		var mogul = this.mogulService.getCurrentMogul();
		var parsedUrl = NoteUrls.parse(url);
		var payload = this.noteService.resolveNotable(mogul.id(), id, type);
		var newNote = this.noteService.create(mogul.id(), payload, parsedUrl, title, note);

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

	@QueryMapping
	Collection<ClientNote> notesForUrl(@Argument String url) {
		var mogul = this.mogulService.getCurrentMogul();
		return this.noteService //
			.notesForUrl(mogul.id(), mogul, NoteUrls.parse(url)) //
			.stream() //
			.map(note -> this.note(MOGUL_TYPE, note)) //
			.toList();
	}

	private ClientNote note(String type, Note note) {
		return new ClientNote(type, note.id(), DateUtils.forDate(note.created()), DateUtils.forDate(note.done()),
				note.url(), note.title(), note.note());
	}

}

record ClientNote(String type, Long id, OffsetDateTime created, OffsetDateTime done, URI url, String title,
		String note) {
}

record NoteCreatedEvent(Note note) {
}

record NoteUpdatedEvent(Note note) {
}
