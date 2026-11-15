package com.joshlong.mogul.api.notes;

import com.joshlong.mogul.api.mogul.MogulService;
import com.joshlong.mogul.api.podcasts.PodcastService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;

@SpringBootTest
class NoteServiceTest {

	@Test
	void notes(@Autowired JdbcClient db, @Autowired MogulService mogulService, @Autowired PodcastService podcastService,
			@Autowired NoteService noteService) {

		var id = db.sql("select id from mogul").query((rs, _) -> rs.getLong("id")).list().getFirst();
		var mogul = mogulService.getMogulById(id);
		Assertions.assertNotNull(mogul, "the mogul is not null");
		var podcasts = podcastService.getAllPodcastsByMogul(mogul.id());
		Assertions.assertFalse(podcasts.isEmpty(), "there should be at least one podcast for this mogul");
		var first = podcasts.iterator().next();
		var existing = noteService.notes(mogul.id(), first, true).size();
		var existingUnfinished = noteService.notes(mogul.id(), first, false).size();
		var noteText = "this is a test note";
		var thisIsATestNote = noteService.create(mogul.id(), first, null, noteText);
		Assertions.assertNotNull(thisIsATestNote, "the note is not null");
		Assertions.assertEquals(noteText, thisIsATestNote.note());
		Assertions.assertEquals(existing + 1, noteService.notes(mogul.id(), first).size());
		var secondNote = noteService.create(mogul.id(), first, null, "this is another test note");
		Assertions.assertEquals(existing + 2, noteService.notes(mogul.id(), first).size());
		var anotherOne = "another one!";
		var updated = noteService.update(mogul.id(), secondNote.id(), null, anotherOne);
		Assertions.assertEquals(anotherOne, updated.note(), "the note was updated");
		var done = noteService.setDone(mogul.id(), updated.id(), true);
		Assertions.assertNotNull(done.done(), "the note was marked done");
		Assertions.assertEquals(existingUnfinished + 1, noteService.notes(mogul.id(), first, false).size(),
				"the done note should be excluded");
		Assertions.assertEquals(existing + 2, noteService.notes(mogul.id(), first, true).size(),
				"all notes should be included");
		var unfinished = noteService.setDone(mogul.id(), updated.id(), false);
		Assertions.assertNull(unfinished.done(), "the note was marked unfinished");
		Assertions.assertEquals(updated, noteService.getNoteById(updated.id()),
				"the search should return the same instance");
		noteService.deleteNote(mogul.id(), thisIsATestNote.id());
		noteService.deleteNote(mogul.id(), secondNote.id());
	}

}
