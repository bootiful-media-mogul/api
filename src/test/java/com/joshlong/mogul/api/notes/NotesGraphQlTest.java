package com.joshlong.mogul.api.notes;

import com.joshlong.mogul.api.mogul.MogulService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.graphql.test.tester.HttpGraphQlTester;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.web.servlet.client.MockMvcWebTestClient;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.context.WebApplicationContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt;
import static org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers.springSecurity;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;

/**
 * drives the notes api through graphql, the way the studio and the clip extension do. it
 * expects the local database (see {@code setup_local_dev_from_prod_db.sh}) and only
 * removes the notes it creates.
 */
@SpringBootTest
class NotesGraphQlTest {

	private static final String OWNER = "google-oauth2|107746898487618710317";

	private static final String INTRUDER = "notes-graphql-test|intruder";

	private final List<Long> createdNoteIds = new ArrayList<>();

	@Autowired
	private WebApplicationContext context;

	@Autowired
	private MogulService mogulService;

	@Autowired
	private JdbcClient db;

	@Autowired
	private TransactionTemplate transactionTemplate;

	private HttpGraphQlTester owner;

	private HttpGraphQlTester intruder;

	private Long ownerId;

	@BeforeEach
	void setUp() {
		this.ownerId = this.mogulService.getMogulByName(OWNER).id();
		this.transactionTemplate
			.executeWithoutResult(_ -> this.mogulService.login(INTRUDER, "notes-graphql-test", null, "In", "Truder"));
		this.owner = this.testerFor(OWNER);
		this.intruder = this.testerFor(INTRUDER);
	}

	@AfterEach
	void tearDown() {
		for (var id : this.createdNoteIds)
			this.db.sql("delete from note where id = ?").param(id).update();
	}

	@Test
	void anotherMogulCannotEditMyNote() {
		var noteId = this.createMogulNote(this.owner, "mine", null);

		var updated = this.intruder.document("mutation($id:Int,$note:String){ updateNote(id:$id, note:$note) }")
			.variable("id", noteId)
			.variable("note", "not yours")
			.execute()
			.path("updateNote")
			.entity(Boolean.class)
			.get();

		Assertions.assertFalse(updated, "another mogul must not be able to update my note");
		Assertions.assertEquals("mine", this.mogulNote(noteId).get("note"));
	}

	@Test
	void clipAUrlToMyMogul() {
		var noteId = this.createMogulNote(this.owner, "read this later", "https://example.com/an-article");

		var note = this.mogulNote(noteId);
		Assertions.assertEquals("read this later", note.get("note"));
		Assertions.assertEquals("https://example.com/an-article", note.get("url"));
	}

	@Test
	void findMyNoteForAUrl() {
		var url = "https://example.com/" + java.util.UUID.randomUUID();
		Assertions.assertTrue(this.notesForUrl(this.owner, url).isEmpty(), "a page i haven't clipped has no note");

		var first = this.createMogulNote(this.owner, "first visit", url);
		var second = this.createMogulNote(this.owner, "second visit", url);
		this.createMogulNote(this.owner, "somewhere else", url + "/elsewhere");

		var mine = this.notesForUrl(this.owner, url);
		Assertions.assertEquals(List.of(second, first),
				mine.stream().map(n -> ((Number) n.get("id")).longValue()).toList(), "newest first, and only this url");
		Assertions.assertEquals("second visit", mine.getFirst().get("note"));

		Assertions.assertTrue(this.notesForUrl(this.intruder, url).isEmpty(),
				"another mogul must not see my notes for a url");
	}

	@SuppressWarnings("unchecked")
	private List<Map<String, Object>> notesForUrl(HttpGraphQlTester tester, String url) {
		return (List<Map<String, Object>>) (List<?>) tester
			.document("query($url:String){ notesForUrl(url:$url){ id title note url created } }")
			.variable("url", url)
			.execute()
			.path("notesForUrl")
			.entityList(Map.class)
			.get();
	}

	@Test
	void clipATitledPageWithNoText() {
		var before = this.noteIds();
		this.owner.document("""
				mutation($id:Int,$title:String,$url:String){
				  createNote(type:"mogul", id:$id, title:$title, url:$url)
				}
				""")
			.variable("id", this.ownerId)
			.variable("title", "An Article Worth Reading")
			.variable("url", "https://example.com/worth-reading")
			.execute()
			.path("createNote")
			.entity(Boolean.class)
			.isEqualTo(true);
		var after = this.noteIds();
		after.removeAll(before);
		Assertions.assertEquals(1, after.size());
		var noteId = after.getFirst();
		this.createdNoteIds.add(noteId);

		var note = this.mogulNote(noteId);
		Assertions.assertEquals("An Article Worth Reading", note.get("title"));
		Assertions.assertEquals("", note.get("note"));
		Assertions.assertEquals("https://example.com/worth-reading", note.get("url"));

		// the studio edits only the text: the clipped title and url have to survive that
		this.owner.document("mutation($id:Int,$note:String){ updateNote(id:$id, note:$note) }")
			.variable("id", noteId)
			.variable("note", "my thoughts")
			.execute()
			.path("updateNote")
			.entity(Boolean.class)
			.isEqualTo(true);
		note = this.mogulNote(noteId);
		Assertions.assertEquals("An Article Worth Reading", note.get("title"));
		Assertions.assertEquals("my thoughts", note.get("note"));
		Assertions.assertEquals("https://example.com/worth-reading", note.get("url"));
	}

	@Test
	void editingTheTextKeepsTheUrl() {
		var noteId = this.createMogulNote(this.owner, "first draft", "https://example.com/keep-me");

		this.owner.document("mutation($id:Int,$note:String){ updateNote(id:$id, note:$note) }")
			.variable("id", noteId)
			.variable("note", "second draft")
			.execute()
			.path("updateNote")
			.entity(Boolean.class)
			.isEqualTo(true);

		var note = this.mogulNote(noteId);
		Assertions.assertEquals("second draft", note.get("note"));
		Assertions.assertEquals("https://example.com/keep-me", note.get("url"), "an edit without a url keeps it");
	}

	@Test
	void editingCanChangeAndClearTheUrl() {
		var noteId = this.createMogulNote(this.owner, "a note", "https://example.com/old");

		this.updateUrl(noteId, "https://example.com/new");
		Assertions.assertEquals("https://example.com/new", this.mogulNote(noteId).get("url"));

		this.updateUrl(noteId, null);
		Assertions.assertNull(this.mogulNote(noteId).get("url"), "an explicit null url clears it");
	}

	@Test
	void theStudioCanEditTheTitleAndUrl() {
		var noteId = this.createMogulNote(this.owner, "text", "https://example.com/before");
		var update = "mutation($id:Int,$note:String,$title:String,$url:String){ updateNote(id:$id, note:$note, title:$title, url:$url) }";

		this.owner.document(update)
			.variable("id", noteId)
			.variable("note", "text")
			.variable("title", "A New Title")
			.variable("url", "https://example.com/after")
			.execute()
			.path("updateNote")
			.entity(Boolean.class)
			.isEqualTo(true);
		var note = this.mogulNote(noteId);
		Assertions.assertEquals("A New Title", note.get("title"));
		Assertions.assertEquals("https://example.com/after", note.get("url"));

		this.owner.document(update)
			.variable("id", noteId)
			.variable("note", "text")
			.variable("title", null)
			.variable("url", null)
			.execute()
			.path("updateNote")
			.entity(Boolean.class)
			.isEqualTo(true);
		note = this.mogulNote(noteId);
		Assertions.assertNull(note.get("title"), "a null title clears it");
		Assertions.assertNull(note.get("url"), "a null url clears it");
	}

	@Test
	void onlyWebUrlsCanBeClipped() {
		var before = this.noteIds();
		for (var bad : List.of("javascript:alert(1)", "not a url", "/relative/path", "file:///etc/passwd")) {
			this.owner
				.document("mutation($id:Int,$url:String){ createNote(type:\"mogul\", id:$id, note:\"x\", url:$url) }")
				.variable("id", this.ownerId)
				.variable("url", bad)
				.execute()
				.errors()
				.satisfy(errors -> Assertions.assertFalse(errors.isEmpty(), "[" + bad + "] should be rejected"));
		}
		Assertions.assertEquals(before, this.noteIds(), "no note should have been created for a bad url");
	}

	private void updateUrl(Long noteId, String url) {
		this.owner.document("mutation($id:Int,$note:String,$url:String){ updateNote(id:$id, note:$note, url:$url) }")
			.variable("id", noteId)
			.variable("note", "a note")
			.variable("url", url)
			.execute()
			.path("updateNote")
			.entity(Boolean.class)
			.isEqualTo(true);
	}

	private Long createMogulNote(HttpGraphQlTester tester, String note, String url) {
		var before = this.noteIds();
		tester.document(
				"mutation($id:Int,$note:String,$url:String){ createNote(type:\"mogul\", id:$id, note:$note, url:$url) }")
			.variable("id", this.ownerId)
			.variable("note", note)
			.variable("url", url)
			.execute()
			.path("createNote")
			.entity(Boolean.class)
			.isEqualTo(true);
		var after = this.noteIds();
		after.removeAll(before);
		Assertions.assertEquals(1, after.size(), "exactly one note should have been created");
		var id = after.getFirst();
		this.createdNoteIds.add(id);
		return id;
	}

	private List<Long> noteIds() {
		return new ArrayList<>(this.db.sql("select id from note").query(Long.class).list());
	}

	@SuppressWarnings("unchecked")
	private Map<String, Object> mogulNote(Long noteId) {
		var notes = this.owner
			.document(
					"query($id:Int){ notesForNotable(type:\"mogul\", id:$id, includeDone:true){ id title note url } }")
			.variable("id", this.ownerId)
			.execute()
			.path("notesForNotable")
			.entityList(Map.class)
			.get();
		return (Map<String, Object>) notes.stream()
			.filter(n -> ((Number) n.get("id")).longValue() == noteId)
			.findFirst()
			.orElseThrow();
	}

	private HttpGraphQlTester testerFor(String subject) {
		var mockMvc = MockMvcBuilders.webAppContextSetup(this.context)
			.apply(springSecurity())
			.defaultRequest(post("/graphql").with(jwt().jwt(j -> j.subject(subject))))
			.build();
		var client = MockMvcWebTestClient.bindTo(mockMvc).baseUrl("/graphql").build();
		return HttpGraphQlTester.create(client);
	}

}
