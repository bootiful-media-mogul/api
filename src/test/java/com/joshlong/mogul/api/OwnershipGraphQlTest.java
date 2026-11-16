package com.joshlong.mogul.api;

import com.joshlong.mogul.api.mogul.MogulService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.graphql.test.tester.GraphQlTester;
import org.springframework.graphql.test.tester.HttpGraphQlTester;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.web.servlet.client.MockMvcWebTestClient;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.context.WebApplicationContext;

import java.util.Map;
import java.util.function.Consumer;

import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt;
import static org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers.springSecurity;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;

/**
 * one mogul must not be able to read or change another's things by guessing their ids,
 * which are sequential. a throwaway victim builds a podcast, an episode, a segment, a
 * blog, and a post through the api, the way the studio does; a throwaway intruder then
 * tries every operation that takes an id against them. each must be refused, and must
 * leave the victim's things as they were. nothing of anyone else's is touched.
 */
@SpringBootTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class OwnershipGraphQlTest {

	private static final String VICTIM = "ownership-graphql-test|victim";

	private static final String INTRUDER = "ownership-graphql-test|intruder";

	@Autowired
	private WebApplicationContext context;

	@Autowired
	private MogulService mogulService;

	@Autowired
	private JdbcClient db;

	@Autowired
	private TransactionTemplate transactionTemplate;

	private HttpGraphQlTester victim;

	private HttpGraphQlTester intruder;

	private long podcastId, episodeId, segmentId, blogId, postId, graphicId, compositionId, attachmentId;

	@BeforeAll
	void buildTheVictimsThings() {
		this.transactionTemplate.executeWithoutResult(_ -> {
			this.mogulService.login(VICTIM, "ownership-graphql-test", null, "Vic", "Tim");
			this.mogulService.login(INTRUDER, "ownership-graphql-test", null, "In", "Truder");
		});
		this.victim = this.testerFor(VICTIM);
		this.intruder = this.testerFor(INTRUDER);

		this.podcastId = this.id(this.victim.document("mutation { createPodcast(title:\"victim's podcast\"){ id } }")
			.execute()
			.path("createPodcast.id"));
		this.episodeId = this.id(this.victim
			.document(
					"""
							mutation($id:Int){ createPodcastEpisodeDraft(podcastId:$id, title:"victim's episode", description:"d"){ id } }
							""")
			.variable("id", this.podcastId)
			.execute()
			.path("createPodcastEpisodeDraft.id"));
		this.segmentId = this
			.id(this.victim.document("mutation($id:Int){ createPodcastEpisodeSegment(podcastEpisodeId:$id) }")
				.variable("id", this.episodeId)
				.execute()
				.path("createPodcastEpisodeSegment"));
		this.blogId = this
			.id(this.victim.document("mutation { createBlog(title:\"victim's blog\", description:\"d\"){ id } }")
				.execute()
				.path("createBlog.id"));
		this.postId = this.id(this.victim
			.document(
					"""
							mutation($id:Int){ createPost(blogId:$id, title:"victim's post", content:"c", summary:"s", rssSlug:"victims-post"){ id } }
							""")
			.variable("id", this.blogId)
			.execute()
			.path("createPost.id"));
		var episode = this.victim.document(
				"query($id:Int){ podcastEpisodeById(podcastEpisodeId:$id){ graphic { id } titleComposition { id } } }")
			.variable("id", this.episodeId)
			.execute();
		this.graphicId = this.id(episode.path("podcastEpisodeById.graphic.id"));
		this.compositionId = this.id(episode.path("podcastEpisodeById.titleComposition.id"));
		this.attachmentId = this
			.id(this.victim.document("mutation($id:Int){ createCompositionAttachment(compositionId:$id){ id } }")
				.variable("id", this.compositionId)
				.execute()
				.path("createCompositionAttachment.id"));
	}

	// podcasts

	@Test
	void podcastById() {
		this.refused("query($id:Int){ podcastById(podcastId:$id){ id title } }", Map.of("id", this.podcastId));
	}

	@Test
	void podcastEpisodesByPodcast() {
		this.refused("query($id:Int){ podcastEpisodesByPodcast(podcastId:$id){ id title } }",
				Map.of("id", this.podcastId));
	}

	@Test
	void podcastEpisodeById() {
		this.refused("query($id:Int){ podcastEpisodeById(podcastEpisodeId:$id){ id title } }",
				Map.of("id", this.episodeId));
	}

	@Test
	void podcastEpisodeSegmentById() {
		this.refused("query($id:Int){ podcastEpisodeSegmentById(podcastEpisodeSegmentId:$id){ id } }",
				Map.of("id", this.segmentId));
	}

	@Test
	void updatePodcast() {
		this.refused("mutation($id:Int){ updatePodcast(podcastId:$id, title:\"pwned\") }",
				Map.of("id", this.podcastId));
		Assertions.assertEquals("victim's podcast", this.podcastTitle());
	}

	@Test
	void createPodcastEpisodeDraft() {
		var before = this.count("select count(*) from podcast_episode where podcast_id = ?", this.podcastId);
		this.refused("""
				mutation($id:Int){ createPodcastEpisodeDraft(podcastId:$id, title:"pwned", description:"d"){ id } }
				""", Map.of("id", this.podcastId));
		Assertions.assertEquals(before,
				this.count("select count(*) from podcast_episode where podcast_id = ?", this.podcastId));
	}

	@Test
	void updatePodcastEpisode() {
		this.refused(
				"mutation($id:Int){ updatePodcastEpisode(podcastEpisodeId:$id, title:\"pwned\", description:\"d\") }",
				Map.of("id", this.episodeId));
		Assertions.assertEquals("victim's episode", this.episodeTitle());
	}

	@Test
	void deletePodcastEpisode() {
		this.refused("mutation($id:Int){ deletePodcastEpisode(podcastEpisodeId:$id) }", Map.of("id", this.episodeId));
		Assertions.assertEquals("victim's episode", this.episodeTitle(), "the episode must still be there");
	}

	@Test
	void createPodcastEpisodeSegment() {
		var before = this.segments();
		this.refused("mutation($id:Int){ createPodcastEpisodeSegment(podcastEpisodeId:$id) }",
				Map.of("id", this.episodeId));
		Assertions.assertEquals(before, this.segments());
	}

	@Test
	void deletePodcastEpisodeSegment() {
		this.refused("mutation($id:Int){ deletePodcastEpisodeSegment(podcastEpisodeSegmentId:$id) }",
				Map.of("id", this.segmentId));
		Assertions.assertEquals(1,
				this.count("select count(*) from podcast_episode_segment where id = ?", this.segmentId));
	}

	@Test
	void movePodcastEpisodeSegmentUp() {
		this.refused("""
				mutation($e:Int,$s:Int){ movePodcastEpisodeSegmentUp(podcastEpisodeId:$e, podcastEpisodeSegmentId:$s) }
				""", Map.of("e", this.episodeId, "s", this.segmentId));
	}

	@Test
	void movePodcastEpisodeSegmentDown() {
		this.refused(
				"""
						mutation($e:Int,$s:Int){ movePodcastEpisodeSegmentDown(podcastEpisodeId:$e, podcastEpisodeSegmentId:$s) }
						""",
				Map.of("e", this.episodeId, "s", this.segmentId));
	}

	// blogs

	@Test
	void blogById() {
		this.refused("query($id:Int){ blogById(blogId:$id){ id title } }", Map.of("id", this.blogId));
	}

	@Test
	void blogPostsByBlog() {
		this.refused("query($id:Int){ blogPostsByBlog(blogId:$id){ id title } }", Map.of("id", this.blogId));
	}

	@Test
	void postById() {
		this.refused("query($id:Int){ postById(postId:$id){ id title } }", Map.of("id", this.postId));
	}

	@Test
	void createPost() {
		var before = this.count("select count(*) from blog_post where blog_id = ?", this.blogId);
		this.refused(
				"""
						mutation($id:Int){ createPost(blogId:$id, title:"pwned", content:"c", summary:"s", rssSlug:"pwned"){ id } }
						""",
				Map.of("id", this.blogId));
		Assertions.assertEquals(before, this.count("select count(*) from blog_post where blog_id = ?", this.blogId));
	}

	@Test
	void updatePost() {
		this.refused("""
				mutation($id:Int){ updatePost(postId:$id, title:"pwned", description:"d", summary:"s", rssSlug:"r") }
				""", Map.of("id", this.postId));
		Assertions.assertEquals("victim's post", this.postTitle());
	}

	@Test
	void setBlogPostVisibility() {
		var before = this.postVisible();
		this.refused("mutation($id:Int){ setBlogPostVisibility(postId:$id, visible:" + !before + ") }",
				Map.of("id", this.postId));
		Assertions.assertEquals(before, this.postVisible());
	}

	@Test
	void deletePost() {
		this.refused("mutation($id:Int){ deletePost(postId:$id) }", Map.of("id", this.postId));
		Assertions.assertEquals("victim's post", this.postTitle(), "the post must still be there");
	}

	@Test
	void deleteBlog() {
		this.refused("mutation($id:Int){ deleteBlog(blogId:$id) }", Map.of("id", this.blogId));
		Assertions.assertEquals(1, this.count("select count(*) from blog where id = ?", this.blogId),
				"the blog must still be there");
	}

	// managed files

	@Test
	void managedFileById() {
		this.refused("query($id:Int){ managedFileById(managedFileId:$id){ id } }", Map.of("id", this.graphicId));
	}

	@Test
	void setManagedFileVisibility() {
		var before = this.graphicVisible();
		this.refused("mutation($id:Int){ setManagedFileVisibility(managedFileId:$id, visible:" + !before + ") }",
				Map.of("id", this.graphicId));
		Assertions.assertEquals(before, this.graphicVisible());
	}

	// compositions

	@Test
	void compositionById() {
		this.refused("query($id:Int){ compositionById(compositionId:$id){ id } }", Map.of("id", this.compositionId));
	}

	@Test
	void createCompositionAttachment() {
		var before = this.count("select count(*) from composition_attachment where composition_id = ?",
				this.compositionId);
		this.refused("mutation($id:Int){ createCompositionAttachment(compositionId:$id){ id } }",
				Map.of("id", this.compositionId));
		Assertions.assertEquals(before,
				this.count("select count(*) from composition_attachment where composition_id = ?", this.compositionId));
	}

	@Test
	void deleteCompositionAttachment() {
		this.refused("mutation($id:Int){ deleteCompositionAttachment(compositionAttachmentId:$id) }",
				Map.of("id", this.attachmentId));
		Assertions.assertEquals(1,
				this.count("select count(*) from composition_attachment where id = ?", this.attachmentId));
	}

	// publications

	@Test
	void publicationsForPublishable() {
		this.refused("query($id:Int){ publicationsForPublishable(type:\"episode\", id:$id){ id } }",
				Map.of("id", this.episodeId));
	}

	@Test
	void canPublish() {
		this.refused(
				"""
						query($id:Int){ canPublish(publishableId:$id, publishableType:"episode", contextJson:"{}", plugin:"podbean") }
						""",
				Map.of("id", this.episodeId));
	}

	// transcripts

	@Test
	void writeTranscript() {
		var transcriptId = this.victimsTranscriptId();
		var before = this.transcript(transcriptId);
		this.refused("mutation($id:Int){ writeTranscript(transcriptId:$id, transcript:\"pwned\") }",
				Map.of("id", transcriptId));
		Assertions.assertEquals(before, this.transcript(transcriptId));
	}

	@Test
	void refreshTranscript() {
		this.refused("mutation($id:Int){ refreshTranscript(transcriptId:$id) }",
				Map.of("id", this.victimsTranscriptId()));
	}

	// context

	@Test
	void entityContext() {
		this.refused("query($id:Int){ entityContext(type:\"episode\", id:$id){ resolvedType context } }",
				Map.of("id", this.episodeId));
	}

	// the victim can still do all of it, so the refusals above are about ownership
	@Test
	void theOwnerCanStillReadTheirThings() {
		this.victim.document("query($id:Int){ podcastEpisodesByPodcast(podcastId:$id){ id } }")
			.variable("id", this.podcastId)
			.execute()
			.path("podcastEpisodesByPodcast")
			.entityList(Map.class)
			.hasSizeGreaterThan(0);
		this.victim.document("query($id:Int){ blogPostsByBlog(blogId:$id){ id } }")
			.variable("id", this.blogId)
			.execute()
			.path("blogPostsByBlog")
			.entityList(Map.class)
			.hasSizeGreaterThan(0);
		this.victim.document("query($id:Int){ postById(postId:$id){ title } }")
			.variable("id", this.postId)
			.execute()
			.path("postById.title")
			.entity(String.class)
			.isEqualTo("victim's post");
		this.victim.document("query($id:Int){ compositionById(compositionId:$id){ id } }")
			.variable("id", this.compositionId)
			.execute()
			.path("compositionById.id")
			.hasValue();
		this.victim.document("query($id:Int){ managedFileById(managedFileId:$id){ id } }")
			.variable("id", this.graphicId)
			.execute()
			.path("managedFileById.id")
			.hasValue();
	}

	private void refused(String document, Map<String, Object> variables) {
		var request = this.intruder.document(document);
		for (var e : variables.entrySet())
			request = request.variable(e.getKey(), e.getValue());
		var errors = (Consumer<java.util.List<org.springframework.graphql.ResponseError>>) list -> Assertions
			.assertFalse(list.isEmpty(), "another mogul must be refused: " + document.strip());
		request.execute().errors().satisfy(errors);
	}

	private long id(GraphQlTester.Path path) {
		return path.entity(Long.class).get();
	}

	private long count(String sql, long id) {
		return this.db.sql(sql).param(id).query(Long.class).single();
	}

	private java.util.List<Long> segments() {
		return this.db.sql("select id from podcast_episode_segment where podcast_episode_id = ? order by id")
			.param(this.episodeId)
			.query(Long.class)
			.list();
	}

	private String podcastTitle() {
		return this.db.sql("select title from podcast where id = ?").param(this.podcastId).query(String.class).single();
	}

	private String episodeTitle() {
		return this.db.sql("select title from podcast_episode where id = ?")
			.param(this.episodeId)
			.query(String.class)
			.single();
	}

	private String postTitle() {
		return this.db.sql("select title from blog_post where id = ?").param(this.postId).query(String.class).single();
	}

	private boolean postVisible() {
		return this.db.sql("select visible from blog_post where id = ?")
			.param(this.postId)
			.query(Boolean.class)
			.single();
	}

	private boolean graphicVisible() {
		return this.db.sql("select visible from managed_file where id = ?")
			.param(this.graphicId)
			.query(Boolean.class)
			.single();
	}

	private long victimsTranscriptId() {
		var victimId = this.mogulService.getMogulByName(VICTIM).id();
		var existing = this.db.sql("select id from transcript where mogul_id = ? order by id limit 1")
			.param(victimId)
			.query(Long.class)
			.optional();
		return existing.orElseGet(() -> this.db.sql("""
				insert into transcript (mogul_id, payload, payload_class, transcript)
				values (?, ?, 'com.joshlong.mogul.api.podcasts.Segment', 'the victim said this')
				returning id
				""").params(victimId, Long.toString(this.segmentId)).query(Long.class).single());
	}

	private String transcript(long transcriptId) {
		return this.db.sql("select transcript from transcript where id = ?")
			.param(transcriptId)
			.query(String.class)
			.optional()
			.orElse(null);
	}

	private HttpGraphQlTester testerFor(String subject) {
		var mockMvc = MockMvcBuilders.webAppContextSetup(this.context)
			.apply(springSecurity())
			.defaultRequest(post("/graphql").with(jwt().jwt(j -> j.subject(subject))))
			.build();
		return HttpGraphQlTester.create(MockMvcWebTestClient.bindTo(mockMvc).baseUrl("/graphql").build());
	}

}
