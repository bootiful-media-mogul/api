package com.joshlong.mogul.api.search;

import com.joshlong.mogul.api.blogs.BlogService;
import com.joshlong.mogul.api.jobs.JobExecutionContext;
import com.joshlong.mogul.api.mogul.MogulService;
import com.joshlong.mogul.api.notes.NoteService;
import com.joshlong.mogul.api.podcasts.PodcastService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.data.elasticsearch.client.elc.NativeQuery;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.data.elasticsearch.core.query.DeleteQuery;
import org.springframework.data.elasticsearch.core.query.IndexQueryBuilder;
import org.springframework.graphql.test.tester.HttpGraphQlTester;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.web.servlet.client.MockMvcWebTestClient;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.context.WebApplicationContext;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.awaitility.Awaitility.await;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt;
import static org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers.springSecurity;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;

/**
 * search, end to end through graphql, against an elasticsearch of its own (in a
 * container): never the real index. two throwaway moguls, alice and bob, each search only
 * their own things, and the index keeps every entry, drops dead ones, and doesn't let one
 * result knock out another.
 */
@SpringBootTest
@Import(SearchScopingTest.Elasticsearch.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SearchScopingTest {

	@TestConfiguration(proxyBeanMethods = false)
	static class Elasticsearch {

		@Bean
		@ServiceConnection
		ElasticsearchContainer elasticsearchContainer() {
			return new ElasticsearchContainer("docker.elastic.co/elasticsearch/elasticsearch:9.4.8")
				.withEnv("xpack.security.enabled", "false")
				.withEnv("ES_JAVA_OPTS", "-Xms512m -Xmx512m");
		}

	}

	private static final String ALICE = "search-scoping-test|alice";

	private static final String BOB = "search-scoping-test|bob";

	private static final String INDEX = "searchables";

	@Autowired
	private WebApplicationContext context;

	@Autowired
	private MogulService mogulService;

	@Autowired
	private NoteService noteService;

	@Autowired
	private BlogService blogService;

	@Autowired
	private SearchService searchService;

	@Autowired
	private PodcastService podcastService;

	@Autowired
	private SearchIndexJob searchIndexJob;

	@Autowired
	private ElasticsearchOperations elasticsearch;

	@Autowired
	private JdbcClient db;

	@Autowired
	private TransactionTemplate transactionTemplate;

	private HttpGraphQlTester alice;

	private HttpGraphQlTester bob;

	private long aliceId;

	private long bobId;

	@BeforeAll
	void moguls() {
		this.transactionTemplate.executeWithoutResult(_ -> {
			this.mogulService.login(ALICE, "search-scoping-test", null, "Al", "Ice");
			this.mogulService.login(BOB, "search-scoping-test", null, "B", "Ob");
		});
		this.aliceId = this.mogulService.getMogulByName(ALICE).id();
		this.bobId = this.mogulService.getMogulByName(BOB).id();
		this.alice = this.testerFor(ALICE);
		this.bob = this.testerFor(BOB);
	}

	@Test
	void eachMogulFindsOnlyTheirOwnThings() {
		var word = this.word();
		var alicesNote = this.createMogulNote(this.alice, this.aliceId, "alice's note about " + word);
		var bobsNote = this.createMogulNote(this.bob, this.bobId, "bob's note about " + word);

		await().atMost(Duration.ofSeconds(30))
			.untilAsserted(() -> Assertions.assertEquals(List.of(alicesNote), this.found(this.alice, word, "note")));
		await().atMost(Duration.ofSeconds(30))
			.untilAsserted(() -> Assertions.assertEquals(List.of(bobsNote), this.found(this.bob, word, "note")));
	}

	@Test
	void aPostAndTheNoteOnItAreBothFound() {
		var word = this.word();
		var blogId = this
			.id(this.alice.document("mutation { createBlog(title:\"alice's blog\", description:\"d\"){ id } }")
				.execute()
				.path("createBlog.id"));
		var postId = this.id(this.alice.document(
				"mutation($id:Int,$t:String){ createPost(blogId:$id, title:$t, content:\"c\", summary:\"s\", rssSlug:\"r\"){ id } }")
			.variable("id", blogId)
			.variable("t", "a post about " + word)
			.execute()
			.path("createPost.id"));
		// a note on the post shares the post's id as the result's aggregate id
		var noteId = this.createNote(this.alice, "post", postId, "a note on the post about " + word);

		await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
			Assertions.assertEquals(List.of(postId), this.found(this.alice, word, "post"), "the post");
			Assertions.assertEquals(List.of(noteId), this.found(this.alice, word, "note"), "and the note on it");
		});
	}

	@Test
	void aNoteAndAPostWithTheSameIdAreBothKept() {
		var word = this.word();
		var blogId = this
			.id(this.alice.document("mutation { createBlog(title:\"alice's other blog\", description:\"d\"){ id } }")
				.execute()
				.path("createBlog.id"));
		// a post and a note with the very same id, which used to share one index entry.
		// written straight to the tables, at an id far past what either has used
		var id = this.unusedId();
		this.db.sql("insert into blog_post (id, blog_id, title, content) values (?, ?, ?, 'c')")
			.params(id, blogId, "a post about " + word)
			.update();
		this.db.sql("""
				insert into note (id, mogul_id, created, payload, payload_class, note)
				values (?, ?, now(), ?, 'com.joshlong.mogul.api.mogul.Mogul', ?)
				""").params(id, this.aliceId, Long.toString(this.aliceId), "a note about " + word).update();
		try {
			this.searchService.index(this.blogService.getPostById(id));
			this.searchService.index(this.noteService.getNoteById(id));
			await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
				Assertions.assertEquals(List.of(id), this.found(this.alice, word, "post"), "the post");
				Assertions.assertEquals(List.of(id), this.found(this.alice, word, "note"), "and the note");
			});
		} //
		finally {
			this.db.sql("delete from note where id = ?").param(id).update();
			this.db.sql("delete from blog_post where id = ?").param(id).update();
		}
	}

	private long unusedId() {
		while (true) {
			var id = 2_100_000_000L + new java.util.Random().nextInt(40_000_000);
			var used = this.db
				.sql("select (select count(*) from note where id = ?) + (select count(*) from blog_post where id = ?)")
				.params(id, id)
				.query(Long.class)
				.single();
			if (used == 0)
				return id;
		}
	}

	@Test
	void aPickerSearchesOneBlogsPostsAndNothingElse() {
		var word = this.word();
		var blogId = this.createBlog(this.alice, "alice's picked blog");
		var otherBlogId = this.createBlog(this.alice, "alice's unpicked blog");
		var postId = this.createPost(this.alice, blogId, "a post about " + word);
		this.createPost(this.alice, otherBlogId, "another post about " + word);
		this.createNote(this.alice, "post", postId, "a note about " + word);

		var onlyThisBlog = Map.<String, Object>of("type", "post", "blogId", blogId);
		await().atMost(Duration.ofSeconds(30))
			.untilAsserted(() -> Assertions.assertEquals(List.of(postId), this.picked(this.alice, word, onlyThisBlog),
					"the post in that blog, and not the other blog's, nor the note on it"));
		Assertions.assertEquals(List.of(), this.picked(this.bob, word, onlyThisBlog),
				"naming alice's blog doesn't get bob her posts");
	}

	@Test
	void aPickerSearchesOnePodcastsEpisodes() {
		var word = this.word();
		var podcastId = this.createPodcast(this.alice, "alice's picked podcast");
		var otherPodcastId = this.createPodcast(this.alice, "alice's unpicked podcast");
		var episodeId = this.createEpisodeWithASegment(this.alice, podcastId, "an episode about " + word);
		this.createEpisodeWithASegment(this.alice, otherPodcastId, "another episode about " + word);

		await().atMost(Duration.ofSeconds(30))
			.untilAsserted(() -> Assertions.assertEquals(List.of(episodeId),
					this.picked(this.alice, word, Map.of("type", "segment", "podcastId", podcastId)),
					"an episode is found by its segments, once per episode, in that podcast only"));
	}

	// what a clip app's picker gets back: the aggregate ids, the posts' or episodes' own
	private List<Long> picked(HttpGraphQlTester tester, String word, Map<String, Object> metadata) {
		var results = tester.document("query($q:String,$m:JSON){ search(query:$q, metadata:$m){ aggregateId } }")
			.variable("q", word)
			.variable("m", metadata)
			.execute()
			.path("search")
			.entityList(Map.class)
			.get();
		return results.stream().map(r -> ((Number) r.get("aggregateId")).longValue()).sorted().toList();
	}

	private long createBlog(HttpGraphQlTester tester, String title) {
		return this.id(tester.document("mutation($t:String){ createBlog(title:$t, description:\"d\"){ id } }")
			.variable("t", title)
			.execute()
			.path("createBlog.id"));
	}

	private long createPost(HttpGraphQlTester tester, long blogId, String title) {
		return this.id(tester.document(
				"mutation($id:Int,$t:String){ createPost(blogId:$id, title:$t, content:\"c\", summary:\"s\", rssSlug:\"r\"){ id } }")
			.variable("id", blogId)
			.variable("t", title)
			.execute()
			.path("createPost.id"));
	}

	private long createPodcast(HttpGraphQlTester tester, String title) {
		return this.id(tester.document("mutation($t:String){ createPodcast(title:$t){ id } }")
			.variable("t", title)
			.execute()
			.path("createPodcast.id"));
	}

	// segments are indexed once they're transcribed; here, straight away
	private long createEpisodeWithASegment(HttpGraphQlTester tester, long podcastId, String title) {
		var episodeId = this.id(tester.document(
				"mutation($id:Int,$t:String){ createPodcastEpisodeDraft(podcastId:$id, title:$t, description:\"d\"){ id } }")
			.variable("id", podcastId)
			.variable("t", title)
			.execute()
			.path("createPodcastEpisodeDraft.id"));
		var segmentId = this
			.id(tester.document("mutation($id:Int){ createPodcastEpisodeSegment(podcastEpisodeId:$id) }")
				.variable("id", episodeId)
				.execute()
				.path("createPodcastEpisodeSegment"));
		this.searchService
			.index(this.podcastService.getPodcastEpisodeSegmentsByIds(List.of(segmentId)).iterator().next());
		return episodeId;
	}

	@Test
	void deletingANoteTakesItOutOfTheIndex() {
		var word = this.word();
		var noteId = this.createMogulNote(this.alice, this.aliceId, "soon gone: " + word);
		await().atMost(Duration.ofSeconds(30))
			.untilAsserted(() -> Assertions.assertEquals(List.of(noteId), this.found(this.alice, word, "note")));

		this.alice.document("mutation($id:Int){ deleteNote(id:$id) }")
			.variable("id", noteId)
			.execute()
			.path("deleteNote")
			.entity(Boolean.class)
			.isEqualTo(true);

		await().atMost(Duration.ofSeconds(30))
			.untilAsserted(() -> Assertions.assertEquals(0, this.entriesMentioning(word), "the index entry is gone"));
	}

	@Test
	void anEntryForSomethingThatIsGoneIsDropped() {
		var word = this.word();
		var noteId = this.createMogulNote(this.alice, this.aliceId, "gone behind its back: " + word);
		await().atMost(Duration.ofSeconds(30))
			.untilAsserted(() -> Assertions.assertEquals(List.of(noteId), this.found(this.alice, word, "note")));

		// gone without the app knowing, the way cascades and old bugs leave entries
		// behind
		this.db.sql("delete from note where id = ?").param(noteId).update();

		Assertions.assertEquals(List.of(), this.found(this.alice, word, "note"), "it isn't a result");
		await().atMost(Duration.ofSeconds(30))
			.untilAsserted(() -> Assertions.assertEquals(0, this.entriesMentioning(word), "and its entry is gone"));
	}

	@Test
	void theSearchIndexJobIsOnTheJobsScreen() {
		var jobs = this.alice.document("query { jobs { name requiredContextAttributes } }")
			.execute()
			.path("jobs")
			.entityList(Map.class)
			.get();
		var job = jobs.stream().filter(j -> "searchIndexJob".equals(j.get("name"))).findFirst().orElseThrow();
		Assertions.assertEquals(List.of(), job.get("requiredContextAttributes"),
				"the mogul comes from the login, so there's nothing to fill in");
	}

	@Test
	void theRebuildRestoresAMogulsEntriesAndClearsOwnerlessOnes() throws Exception {
		var word = this.word();
		var noteId = this.createMogulNote(this.alice, this.aliceId, "rebuilt: " + word);
		await().atMost(Duration.ofSeconds(30))
			.untilAsserted(() -> Assertions.assertEquals(List.of(noteId), this.found(this.alice, word, "note")));

		// an index that's lost everything, but for an entry from before entries had
		// owners
		var index = IndexCoordinates.of(INDEX);
		this.elasticsearch.delete(
				DeleteQuery.builder(NativeQuery.builder().withQuery(q -> q.matchAll(m -> m)).build()).build(),
				Map.class, index);
		this.elasticsearch.index(new IndexQueryBuilder().withId("legacy-" + word)
			.withSource("{\"searchableId\": " + noteId + ", \"title\": \"legacy " + word
					+ "\", \"description\": \"\", \"className\": \"com.joshlong.mogul.api.Note\"}")
			.build(), index);
		Assertions.assertEquals(1, this.entriesMentioning(word));
		Assertions.assertEquals(List.of(), this.found(this.alice, word, "note"));

		var context = Mockito.mock(JobExecutionContext.class);
		Mockito.when(context.mogulId()).thenReturn(this.aliceId);
		this.searchIndexJob.run(context);

		Assertions.assertEquals(List.of(noteId), this.found(this.alice, word, "note"), "alice's note is back");
		Assertions.assertEquals(1, this.entriesMentioning(word), "and the ownerless entry is gone");
	}

	@Test
	void theRebuildMakesTheIndexIfItsBeenDeleted() throws Exception {
		// the way to change the index's mapping: delete it, and rebuild
		var word = this.word();
		var noteId = this.createMogulNote(this.alice, this.aliceId, "rebuilt from nothing: " + word);
		await().atMost(Duration.ofSeconds(30))
			.untilAsserted(() -> Assertions.assertEquals(List.of(noteId), this.found(this.alice, word, "note")));
		this.elasticsearch.indexOps(IndexCoordinates.of(INDEX)).delete();

		var context = Mockito.mock(JobExecutionContext.class);
		Mockito.when(context.mogulId()).thenReturn(this.aliceId);
		this.searchIndexJob.run(context);

		Assertions.assertEquals(List.of(noteId), this.found(this.alice, word, "note"));
		var description = (Map<?, ?>) ((Map<?, ?>) this.elasticsearch.indexOps(IndexCoordinates.of(INDEX))
			.getMapping()
			.get("properties")).get("description");
		Assertions.assertEquals("english", description.get("analyzer"), "made with the mapping the code declares");
	}

	@Test
	void theRebuildReplacesAnIndexFromBeforeTheEnglishAnalyzer() throws Exception {
		// production's index was made before the fields had the english analyzer, and
		// elasticsearch won't change a field's analyzer, so rebuilding into it failed
		// with
		// "Cannot update parameter [analyzer] from [default] to [english]"
		var index = this.elasticsearch.indexOps(IndexCoordinates.of(INDEX));
		index.delete();
		index.create();
		index.putMapping(org.springframework.data.elasticsearch.core.document.Document.parse("""
				{"properties": {"title": {"type": "text"}, "description": {"type": "text"},
				 "className": {"type": "text"}, "searchableId": {"type": "long"}}}"""));
		var word = this.word();
		var noteId = this.createMogulNote(this.alice, this.aliceId, "rebuilt over the old index: " + word);

		var context = Mockito.mock(JobExecutionContext.class);
		Mockito.when(context.mogulId()).thenReturn(this.aliceId);
		this.searchIndexJob.run(context);

		Assertions.assertEquals(List.of(noteId), this.found(this.alice, word, "note"));
		var description = (Map<?, ?>) ((Map<?, ?>) index.getMapping().get("properties")).get("description");
		Assertions.assertEquals("english", description.get("analyzer"));
	}

	private List<Long> found(HttpGraphQlTester tester, String word, String type) {
		var results = tester.document("query($q:String){ search(query:$q, metadata:{}){ searchableId type } }")
			.variable("q", word)
			.execute()
			.path("search")
			.entityList(Map.class)
			.get();
		return results.stream()
			.filter(r -> type.equals(r.get("type")))
			.map(r -> ((Number) r.get("searchableId")).longValue())
			.sorted()
			.toList();
	}

	private long entriesMentioning(String word) {
		this.elasticsearch.indexOps(IndexCoordinates.of(INDEX)).refresh();
		var query = NativeQuery.builder()
			.withQuery(q -> q.multiMatch(m -> m.query(word).fields("title", "description")))
			.build();
		return this.elasticsearch.count(query, IndexCoordinates.of(INDEX));
	}

	private long createMogulNote(HttpGraphQlTester tester, long mogulId, String text) {
		return this.createNote(tester, "mogul", mogulId, text);
	}

	private long createNote(HttpGraphQlTester tester, String type, long notableId, String text) {
		var before = this.noteIds();
		tester.document("mutation($t:String,$id:Int,$n:String){ createNote(type:$t, id:$id, note:$n) }")
			.variable("t", type)
			.variable("id", notableId)
			.variable("n", text)
			.execute()
			.path("createNote")
			.entity(Boolean.class)
			.isEqualTo(true);
		var after = this.noteIds();
		after.removeAll(before);
		Assertions.assertEquals(1, after.size());
		return after.getFirst();
	}

	private List<Long> noteIds() {
		return new java.util.ArrayList<>(this.db.sql("select id from note").query(Long.class).list());
	}

	private long id(org.springframework.graphql.test.tester.GraphQlTester.Path path) {
		return path.entity(Long.class).get();
	}

	// a word that matches nothing else, so each test finds only what it made
	private String word() {
		return "zq" + UUID.randomUUID().toString().replace("-", "").substring(0, 12);
	}

	private HttpGraphQlTester testerFor(String subject) {
		var mockMvc = MockMvcBuilders.webAppContextSetup(this.context)
			.apply(springSecurity())
			.defaultRequest(post("/graphql").with(jwt().jwt(j -> j.subject(subject))))
			.build();
		return HttpGraphQlTester.create(MockMvcWebTestClient.bindTo(mockMvc).baseUrl("/graphql").build());
	}

}
