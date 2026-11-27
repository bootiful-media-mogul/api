package com.joshlong.mogul.api.notes;

import com.joshlong.mogul.api.Note;
import com.joshlong.mogul.api.mogul.MogulService;
import com.joshlong.mogul.utils.DateUtils;
import org.springframework.context.ApplicationEventPublisher;
import com.joshlong.mogul.utils.TypeUtils;
import org.springframework.graphql.data.ArgumentValue;
import org.springframework.graphql.data.method.annotation.Argument;
import org.springframework.graphql.data.method.annotation.MutationMapping;
import org.springframework.graphql.data.method.annotation.QueryMapping;
import org.springframework.stereotype.Controller;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.Collection;

@Controller
class NotesController {

	private final MogulService mogulService;

	private final NoteService noteService;

	private final ApplicationEventPublisher publisher;

	private final NoteSummaries noteSummaries;

	private final NoteSummarizer noteSummarizer;

	NotesController(MogulService mogulService, NoteService noteService, ApplicationEventPublisher publisher,
			NoteSummaries noteSummaries, NoteSummarizer noteSummarizer) {
		this.mogulService = mogulService;
		this.noteService = noteService;
		this.publisher = publisher;
		this.noteSummaries = noteSummaries;
		this.noteSummarizer = noteSummarizer;
	}

	/**
	 * a title for a note with this text. the note editor asks for one when a summary has
	 * filled in a note that has no title.
	 */
	@QueryMapping
	String noteTitle(@Argument String description) {
		var mogul = this.mogulService.getCurrentMogul();
		return StringUtils.hasText(description) ? this.noteSummarizer.title(description, mogul.language()) : null;
	}

	@MutationMapping
	boolean deleteNote(@Argument Long id) {
		var currentMogul = this.mogulService.getCurrentMogul();
		return this.noteService.deleteNote(currentMogul.id(), id);
	}

	@MutationMapping
	boolean updateNote(@Argument Long id, @Argument String note, ArgumentValue<String> url, ArgumentValue<String> title,
			@Argument String notableType, @Argument Long notableId) {
		var mogul = this.mogulService.getCurrentMogul();
		// leaving url or title out keeps what the note has: the studio edits only the
		// text, and must not wipe what the clipper saved
		var existing = (url.isOmitted() || title.isOmitted()) ? this.noteService.getNoteById(id) : null;
		if ((url.isOmitted() || title.isOmitted()) && (existing == null || !existing.mogulId().equals(mogul.id())))
			return false;
		var newUrl = url.isOmitted() ? existing.url() : NoteUrls.parse(url.value());
		var newTitle = title.isOmitted() ? existing.title() : title.value();
		// a notableType and notableId move the note there; leaving them out keeps it put
		Assert.isTrue((notableType == null) == (notableId == null),
				"to move a note, give both its notableType and its notableId");
		var notable = notableType == null ? null : this.noteService.resolveNotable(mogul.id(), notableId, notableType);
		return this.noteService.update(mogul.id(), id, notable, newUrl, newTitle, note) != null;
	}

	/**
	 * asks for a summary of the page at the url, for the note editor. it arrives later as
	 * a {@code page-summarized-event} notification keyed by what this returns.
	 */
	@MutationMapping
	String summarizePage(@Argument String url) {
		var mogul = this.mogulService.getCurrentMogul();
		var parsed = NoteUrls.parse(url);
		Assert.notNull(parsed, "there's no url to summarize");
		return this.noteSummaries.summarizePage(mogul.id(), parsed);
	}

	@MutationMapping
	boolean setNoteDone(@Argument Long id, @Argument Boolean done) {
		var currentMogul = this.mogulService.getCurrentMogul();
		return this.noteService.setDone(currentMogul.id(), id, Boolean.TRUE.equals(done)) != null;
	}

	/**
	 * @param summarize whether a note with a url and no text -- a clip -- should have a
	 * summary of the page written into it. leaving it out means yes, which is what the
	 * clippers want; the studio says no, and summarizes in its editor when asked to.
	 */
	@MutationMapping
	boolean createNote(@Argument String type, @Argument Long id, @Argument String note, @Argument String url,
			@Argument String title, @Argument Boolean summarize) {
		var mogul = this.mogulService.getCurrentMogul();
		var parsedUrl = NoteUrls.parse(url);
		var payload = this.noteService.resolveNotable(mogul.id(), id, type);
		var newNote = this.noteService.create(mogul.id(), payload, parsedUrl, title, note);
		if (newNote != null && !Boolean.FALSE.equals(summarize))
			this.noteSummaries.created(newNote);
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
			.notesForUrl(mogul.id(), NoteUrls.parse(url)) //
			.stream() //
			.map(note -> this.note(TypeUtils.typeName(note.payloadClass()), note)) //
			.toList();
	}

	private ClientNote note(String type, Note note) {
		return new ClientNote(type, Long.parseLong(note.payload()), note.id(), DateUtils.forDate(note.created()),
				DateUtils.forDate(note.done()), note.url(), note.title(), note.note());
	}

}

// type and notableId say what the note is attached to: "mogul", "podcast", "episode",
// "post"
record ClientNote(String type, Long notableId, Long id, OffsetDateTime created, OffsetDateTime done, URI url,
		String title, String note) {
}

record NoteCreatedEvent(Note note) {
}

record NoteUpdatedEvent(Note note) {
}
