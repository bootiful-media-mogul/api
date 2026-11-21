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
import java.util.List;
import java.util.Map;

import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt;
import static org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers.springSecurity;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;

/**
 * the clip apps' post and episode pickers: a page of the newest, so that a blog with
 * thousands of posts never has to be loaded whole (older ones are found with search; see
 * {@code SearchScopingTest}). it expects the local database (see
 * {@code setup_local_dev_from_prod_db.sh}) and changes nothing in it.
 */
@SpringBootTest
class PickerGraphQlTest {

	private static final String OWNER = "google-oauth2|107746898487618710317";

	private static final String INTRUDER = "picker-graphql-test|intruder";

	private static final String POSTS = """
			query($id:Int,$limit:Int){ blogPostsByBlog(blogId:$id, limit:$limit) { id title created } }""";

	private static final String EPISODES = """
			query($id:Int,$limit:Int){ podcastEpisodesByPodcast(podcastId:$id, limit:$limit) { id title created } }""";

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
		var page = this.list(this.owner, POSTS, "blogPostsByBlog", blogId, 100);
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
		Assertions.assertEquals(all, this.list(this.owner, POSTS, "blogPostsByBlog", blogId, null).size(),
				"the studio and the chrome extension ask for no limit, and still get every post");
	}

	@Test
	void aLimitGivesTheNewestEpisodes() {
		// the local database may have no episodes, so the test brings its own, and
		// takes them away again
		var podcastId = this.ownerId("select p.id from podcast p where p.mogul_id = ? order by p.id limit 1");
		var older = this.podcastService.createPodcastEpisodeDraft(this.ownerId, podcastId, "picker older", "");
		var newer = this.podcastService.createPodcastEpisodeDraft(this.ownerId, podcastId, "picker newer", "");
		try {
			this.db.sql("update podcast_episode set created = created - interval '1 day' where id = ?")
				.param(older.id())
				.update();
			var page = this.list(this.owner, EPISODES, "podcastEpisodesByPodcast", podcastId, 1);
			Assertions.assertEquals(List.of(newer.id()), page.stream().map(this::id).toList(),
					"a limit of one gives the newest episode");
		} //
		finally {
			this.podcastService.deletePodcastEpisode(older.id());
			this.podcastService.deletePodcastEpisode(newer.id());
		}
	}

	@Test
	void anotherMogulCannotListMyPostsOrEpisodes() {
		var blogId = this.biggestBlog();
		this.intruder.document(POSTS)
			.variable("id", blogId)
			.variable("limit", 100)
			.execute()
			.errors()
			.satisfy(errors -> Assertions.assertFalse(errors.isEmpty(), "my posts must not be listed for others"));
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
			Integer limit) {
		return tester.document(document)
			.variable("id", id)
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
