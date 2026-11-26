package com.joshlong.mogul.api.notes;

import com.joshlong.mogul.api.Note;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.support.GeneratedKeyHolder;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * how a summary is written into a note, against the local database (see
 * {@code setup_local_dev_from_prod_db.sh}) but without the rest of the application. it
 * only removes the note it makes.
 */
class NoteSummaryWritingTest {

	private final JdbcClient db = JdbcClient.create(new DriverManagerDataSource(
			"jdbc:postgresql://" + System.getenv().getOrDefault("DB_HOST", "localhost") + "/mogul", "mogul", "mogul"));

	private final List<Object> events = new ArrayList<>();

	private final DefaultNoteService notes = new DefaultNoteService(List.of(), this.events::add, this.db, null);

	private Long mogulId;

	private Long noteId;

	@BeforeEach
	void createNote() {
		this.mogulId = this.db.sql("select min(id) from mogul").query(Long.class).single();
	}

	@AfterEach
	void deleteNote() {
		if (this.noteId != null)
			this.db.sql("delete from note where id = ?").param(this.noteId).update();
	}

	private Note note(String text) {
		var keys = new GeneratedKeyHolder();
		this.db.sql("""
				insert into note (mogul_id, payload, payload_class, url, note)
				values (?, ?, 'com.joshlong.mogul.api.mogul.Mogul', 'https://example.com/', ?)
				""").params(this.mogulId, this.mogulId.toString(), text).update(keys);
		this.noteId = ((Number) Objects.requireNonNull(keys.getKeys()).get("id")).longValue();
		return this.notes.getNoteById(this.noteId);
	}

	@Test
	void anEmptyTitleIsFilledIn() {
		var note = this.note("");
		var updated = this.notes.fillTitle(this.mogulId, note.id(), " A Title ");
		assertThat(updated.title()).isEqualTo("A Title");
		assertThat(this.events).singleElement().isInstanceOf(NoteUpdatedEvent.class);
	}

	@Test
	void aTitleIsNeverReplaced() {
		var note = this.note("");
		this.db.sql("update note set title = 'Mine' where id = ?").param(note.id()).update();
		assertThat(this.notes.fillTitle(this.mogulId, note.id(), "The model's")).isNull();
		assertThat(this.notes.getNoteById(note.id()).title()).isEqualTo("Mine");
		assertThat(this.notes.fillTitle(this.mogulId + 1_000_000, note.id(), "Not yours")).isNull();
		assertThat(this.events).isEmpty();
	}

	@Test
	void theHashIsPostgresMd5() {
		var text = "Café ☕ - a note with 🎙 and\nnewlines";
		var note = this.note(text);
		var md5 = this.db.sql("select md5(note) from note where id = ?").param(note.id()).query(String.class).single();
		assertThat(NoteService.textHash(note.note())).isEqualTo(md5);
	}

	@Test
	void fillsInANoteWithNoText() {
		var note = this.note("");
		var updated = this.notes.appendSummary(this.mogulId, note.id(), NoteService.textHash(""), "The summary.");
		assertThat(updated.note()).isEqualTo("The summary.");
		assertThat(this.events).singleElement().isInstanceOf(NoteUpdatedEvent.class);
	}

	@Test
	void fillsInANoteWithOnlyWhitespace() {
		var note = this.note(" \n\t\n");
		var updated = this.notes.appendSummary(this.mogulId, note.id(), NoteService.textHash(note.note()),
				"The summary.");
		assertThat(updated.note()).isEqualTo("The summary.");
	}

	@Test
	void goesUnderTextThatIsThere() {
		var note = this.note("My own words.\n\n");
		var updated = this.notes.appendSummary(this.mogulId, note.id(), NoteService.textHash(note.note()),
				"The summary.");
		assertThat(updated.note()).isEqualTo("My own words.\n\nThe summary.");
	}

	@Test
	void isWrittenOnlyOnce() {
		var note = this.note("");
		var hash = NoteService.textHash("");
		assertThat(this.notes.appendSummary(this.mogulId, note.id(), hash, "The summary.")).isNotNull();
		// the same reply again, as a redelivery would bring it
		assertThat(this.notes.appendSummary(this.mogulId, note.id(), hash, "The summary.")).isNull();
		assertThat(this.notes.getNoteById(note.id()).note()).isEqualTo("The summary.");
	}

	@Test
	void doesNotLandOnAnEditItNeverSaw() {
		var note = this.note("");
		var hash = NoteService.textHash("");
		this.db.sql("update note set note = 'Typed while it was summarizing.' where id = ?").param(note.id()).update();
		assertThat(this.notes.appendSummary(this.mogulId, note.id(), hash, "The summary.")).isNull();
		assertThat(this.notes.getNoteById(note.id()).note()).isEqualTo("Typed while it was summarizing.");
		assertThat(this.events).isEmpty();
	}

	@Test
	void isOnlyWrittenToTheMogulsOwnNote() {
		var note = this.note("");
		assertThat(this.notes.appendSummary(this.mogulId + 1_000_000, note.id(), NoteService.textHash(""), "Mine now."))
			.isNull();
		assertThat(this.notes.getNoteById(note.id()).note()).isEmpty();
	}

}
