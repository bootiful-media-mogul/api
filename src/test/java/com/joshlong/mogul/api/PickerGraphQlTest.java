package com.joshlong.mogul.api;

import com.joshlong.mogul.api.mogul.MogulService;
import com.joshlong.mogul.api.podcasts.PodcastService;
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

import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt;
import static org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers.springSecurity;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;

/**
 * the clip apps' post and episode pickers: a page of the newest, and a title search, so
 * that a blog with thousands of posts never has to be loaded whole. it expects the local
 * database (see {@code setup_local_dev_from_prod_db.sh}) and changes nothing in it.
 */
@SpringBootTest
class PickerGraphQlTest {

	private static final String OWNER = "google-oauth2|107746898487618710317";

	private static final String INTRUDER = "picker-graphql-test|intruder";

	private static final String POSTS = """
			query($id:Int,$query:String,$limit:Int){ blogPostsByBlog(blogId:$id, query:$query, limit:$limit) { id title created } }""";

	private static final String EPISODES = """
			query($id:Int,$query:String,$limit:Int){ podcastEpisodesByPodcast(podcastId:$id, query:$query, limit:$limit) { id title created } }""";

	@Autowired
	private WebApplicationContext context;

	@Autowired
	private MogulService mogulService;

	@Autowired
	private JdbcClient db;

	@Autowired
	private PodcastService podcastService;

	@Autowired
	private TransactionTemplate transactionTemplate;

	private HttpGraphQlTester owner;

	private HttpGraphQlTester intruder;

	private Long ownerId;

	@BeforeEach
	void setUp() {
		this.ownerId = this.mogulService.getMogulByName(OWNER).id();
		this.transactionTemplate
			.executeWithoutResult(_ -> this.mogulService.login(INTRUDER, "picker-graphql-test", null, "In", "Truder"));
		this.owner = this.testerFor(OWNER);
		this.intruder = this.testerFor(INTRUDER);
	}

	@Test
	void aLimitGivesTheNewestPosts() {
		var blogId = this.biggestBlog();
		var page = this.list(this.owner, POSTS, "blogPostsByBlog", blogId, null, 100);
		var all = this.db.sql("select count(*) from blog_post where blog_id = ?")
			.param(blogId)
			.query(Long.class)
			.single();
		Assertions.assertEquals(Math.min(all, 100), page.size());
		this.assertNewestFirst(page);
		var newest = this.db.sql("select id from blog_post where blog_id = ? order by created desc, id desc limit 1")
			.param(blogId)
			.query(Long.class)
			.single();
		Assertions.assertEquals(newest, this.id(page.getFirst()));
	}

	@Test
	void everyPostIsStillThereWithoutALimit() {
		var blogId = this.biggestBlog();
		var all = this.db.sql("select count(*) from blog_post where blog_id = ?")
			.param(blogId)
			.query(Long.class)
			.single();
		Assertions.assertEquals(all, this.list(this.owner, POSTS, "blogPostsByBlog", blogId, null, null).size(),
				"the studio and the chrome extension ask for no limit, and still get every post");
	}

	@Test
	void aPostIsFoundByWordsOfItsTitleInAnyOrderAndCase() {
		var blogId = this.biggestBlog();
		var post = this.db.sql("""
				select id, title from blog_post where blog_id = ? and title ~ '\\w+\\s+\\w+'
				order by created asc, id asc limit 1""")
			.param(blogId)
			.query((rs, _) -> Map.of("id", rs.getLong("id"), "title", rs.getString("title")))
			.single();
		var words = Arrays.stream(((String) post.get("title")).split("\\s+")).filter(w -> w.matches("\\w+")).toList();
		Assertions.assertTrue(words.size() >= 2, "the oldest post with two words in its title: " + post);
		var query = words.get(1).toUpperCase(Locale.ROOT) + " " + words.get(0).toLowerCase(Locale.ROOT);

		var found = this.list(this.owner, POSTS, "blogPostsByBlog", blogId, query, 100);
		Assertions.assertTrue(found.stream().anyMatch(p -> this.id(p) == (long) post.get("id")),
				"[" + query + "] should find " + post + ", oldest post or not");
		for (var p : found)
			for (var word : List.of(words.get(0), words.get(1)))
				Assertions.assertTrue(
						((String) p.get("title")).toLowerCase(Locale.ROOT).contains(word.toLowerCase(Locale.ROOT)),
						p.get("title") + " should have " + word + " in it");
		this.assertNewestFirst(found);
	}

	@Test
	void wildcardsAreTypedLiterally() {
		var blogId = this.biggestBlog();
		var found = this.list(this.owner, POSTS, "blogPostsByBlog", blogId, "%_", 100);
		for (var p : found)
			Assertions.assertTrue(((String) p.get("title")).contains("%_"), p.get("title") + " has no %_ in it");
	}

	@Test
	void anEpisodeIsFoundByWordsOfItsTitle() {
		// the local database may have no episodes, so the test brings its own, and
		// takes them away again
		var podcastId = this.ownerId("select p.id from podcast p where p.mogul_id = ? order by p.id limit 1");
		var tag = UUID.randomUUID().toString().replace("-", "");
		var older = this.podcastService.createPodcastEpisodeDraft(this.ownerId, podcastId, "Older aardvark " + tag, "");
		var newer = this.podcastService.createPodcastEpisodeDraft(this.ownerId, podcastId, "Newer zebra " + tag, "");
		try {
			this.db.sql("update podcast_episode set created = created - interval '1 day' where id = ?")
				.param(older.id())
				.update();
			var page = this.list(this.owner, EPISODES, "podcastEpisodesByPodcast", podcastId, null, 1);
			Assertions.assertEquals(List.of(newer.id()), page.stream().map(this::id).toList(),
					"a limit of one gives the newest episode");

			var found = this.list(this.owner, EPISODES, "podcastEpisodesByPodcast", podcastId,
					tag.toUpperCase(Locale.ROOT) + " AARDVARK", null);
			Assertions.assertEquals(List.of(older.id()), found.stream().map(this::id).toList(),
					"the words of its title, in any order and case, find the older episode alone");

			var both = this.list(this.owner, EPISODES, "podcastEpisodesByPodcast", podcastId, tag, 10);
			Assertions.assertEquals(List.of(newer.id(), older.id()), both.stream().map(this::id).toList(),
					"matches come newest first");
		}
		finally {
			this.podcastService.deletePodcastEpisode(older.id());
			this.podcastService.deletePodcastEpisode(newer.id());
		}
	}

	@Test
	void anotherMogulCannotSearchMyPostsOrEpisodes() {
		var blogId = this.biggestBlog();
		this.intruder.document(POSTS)
			.variable("id", blogId)
			.variable("query", "a")
			.variable("limit", 100)
			.execute()
			.errors()
			.satisfy(errors -> Assertions.assertFalse(errors.isEmpty(), "my posts must not be searchable by others"));
		var podcastId = this.ownerId("select p.id from podcast p where p.mogul_id = ? order by p.id limit 1");
		this.intruder.document(EPISODES)
			.variable("id", podcastId)
			.variable("limit", 100)
			.execute()
			.errors()
			.satisfy(errors -> Assertions.assertFalse(errors.isEmpty(), "my episodes must not be listed for others"));
	}

	private Long biggestBlog() {
		return this.ownerId("""
				select b.id from blog b join blog_post bp on bp.blog_id = b.id where b.mogul_id = ?
				group by b.id order by count(*) desc limit 1""");
	}

	private void assertNewestFirst(List<Map<String, Object>> items) {
		for (var i = 1; i < items.size(); i++) {
			var before = OffsetDateTime.parse((String) items.get(i - 1).get("created"));
			var after = OffsetDateTime.parse((String) items.get(i).get("created"));
			Assertions.assertFalse(after.isAfter(before), items.get(i) + " is newer than " + items.get(i - 1));
		}
	}

	private long id(Map<String, Object> item) {
		return ((Number) item.get("id")).longValue();
	}

	@SuppressWarnings("unchecked")
	private List<Map<String, Object>> list(HttpGraphQlTester tester, String document, String field, Long id,
			String query, Integer limit) {
		return tester.document(document)
			.variable("id", id)
			.variable("query", query)
			.variable("limit", limit)
			.execute()
			.path(field)
			.entityList(Map.class)
			.get()
			.stream()
			.map(m -> (Map<String, Object>) m)
			.toList();
	}

	private Long ownerId(String sql) {
		return this.db.sql(sql).param(this.ownerId).query(Long.class).single();
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
