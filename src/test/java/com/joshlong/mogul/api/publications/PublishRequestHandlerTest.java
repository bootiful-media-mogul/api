package com.joshlong.mogul.api.publications;

import com.joshlong.mogul.api.Publication;
import com.joshlong.mogul.api.PublisherPlugin;
import com.joshlong.mogul.api.mogul.MogulService;
import com.joshlong.mogul.api.podcasts.Episode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * every publication goes through jobrunr now: asking for one leaves it pending and a job
 * in jobrunr's tables, and the job -- run here by hand, because the tests don't start
 * jobrunr's background server -- publishes it as the mogul who asked, with whatever the
 * request carried.
 */
@SpringBootTest
class PublishRequestHandlerTest {

	private static final String PLUGIN_NAME = "publishRequestHandlerTestPlugin";

	private static final String TOKEN_KEY = "wordpress-token";

	private final List<Long> publicationIds = new ArrayList<>();

	@Autowired
	private JdbcClient db;

	@Autowired
	private PublicationService publicationService;

	@Autowired
	private PublishRequestHandler handler;

	@Autowired
	private MogulService mogulService;

	@Autowired
	private RecordingPlugin plugin;

	private Long mogulId;

	private String username;

	private Episode episode;

	@BeforeEach
	void setUp() {
		var row = this.db.sql("""
				select e.id as episode_id, p.mogul_id
				from podcast_episode e join podcast p on p.id = e.podcast_id
				order by e.id desc limit 1
				""").query((rs, _) -> new long[] { rs.getLong("episode_id"), rs.getLong("mogul_id") }).single();
		this.mogulId = row[1];
		this.username = this.mogulService.getMogulById(this.mogulId).username();
		this.authenticate(Map.of(TOKEN_KEY, "a-wordpress-token"));
		this.episode = this.publicationService.resolvePublishable(this.mogulId, row[0], Episode.class);
		this.plugin.calls.clear();
	}

	@AfterEach
	void tearDown() {
		SecurityContextHolder.clearContext();
		for (var id : this.publicationIds) {
			this.db.sql("delete from jobrunr_jobs where jobasjson like ?").params(this.jobJsonPattern(id)).update();
			this.db.sql("delete from publication_outcome where publication_id = ?").params(id).update();
			this.db.sql("delete from publication where id = ?").params(id).update();
		}
	}

	@Test
	void aScheduledPublicationIsPendingUntilItsJobRunsAndThenPublishesAsTheMogul() {
		var when = Instant.now().plus(Duration.ofDays(1)).truncatedTo(ChronoUnit.SECONDS);
		var pending = this.publish(when);

		assertThat(pending.state()).isEqualTo(Publication.State.PENDING);
		assertThat(pending.scheduled().toInstant()).isEqualTo(when);
		assertThat(this.jobState(pending.id())).isEqualTo("SCHEDULED");
		assertThat(this.plugin.calls).as("nothing is published until the job runs").isEmpty();

		// jobrunr's worker thread has nobody authenticated on it
		SecurityContextHolder.clearContext();
		this.handler.run(new PublishRequest(pending.id(), this.mogulId, false));

		assertThat(this.plugin.calls).hasSize(1);
		var call = this.plugin.calls.getFirst();
		assertThat(call.username()).isEqualTo(this.username);
		assertThat(call.token()).isEqualTo("a-wordpress-token");
		assertThat(call.context()).containsEntry("aClientKey", "aClientValue")
			.containsEntry(PublicationService.PUBLICATION_ID, Long.toString(pending.id()))
			.containsEntry(PublicationService.MOGUL_ID, Long.toString(this.mogulId));
		assertThat(SecurityContextHolder.getContext().getAuthentication())
			.as("the worker thread is left the way it was found")
			.isNull();

		var published = this.publicationService.getPublicationById(pending.id());
		assertThat(published.state()).isEqualTo(Publication.State.PUBLISHED);
		assertThat(published.outcomes()).hasSize(1);
		assertThat(published.outcomes().getFirst().success()).isTrue();
	}

	@Test
	void aPublicationForRightNowIsEnqueuedRatherThanRunOnTheRequestThread() {
		var pending = this.publish(null);
		assertThat(pending.state()).isEqualTo(Publication.State.PENDING);
		assertThat(pending.scheduled()).isNull();
		assertThat(this.jobState(pending.id())).isEqualTo("ENQUEUED");
		assertThat(this.plugin.calls).isEmpty();
	}

	@Test
	void aReplayedJobDoesNotPublishTwice() {
		var pending = this.publish(null);
		SecurityContextHolder.clearContext();
		this.handler.run(new PublishRequest(pending.id(), this.mogulId, false));
		this.handler.run(new PublishRequest(pending.id(), this.mogulId, false));
		assertThat(this.plugin.calls).hasSize(1);
	}

	private Publication publish(Instant when) {
		var context = new java.util.HashMap<String, String>();
		context.put("aClientKey", "aClientValue");
		var publication = this.publicationService.publish(this.mogulId, this.episode, context, this.plugin, when);
		this.publicationIds.add(publication.id());
		return publication;
	}

	private String jobState(Long publicationId) {
		return this.db.sql("select state from jobrunr_jobs where jobasjson like ?")
			.params(this.jobJsonPattern(publicationId))
			.query(String.class)
			.single();
	}

	private String jobJsonPattern(Long publicationId) {
		return "%" + PublishRequest.class.getName() + "%\"publicationId\":" + publicationId + ",%";
	}

	private void authenticate(Map<String, Object> details) {
		var authentication = UsernamePasswordAuthenticationToken.authenticated(this.username, null, List.of());
		authentication.setDetails(details);
		var context = SecurityContextHolder.createEmptyContext();
		context.setAuthentication(authentication);
		SecurityContextHolder.setContext(context);
	}

	record Call(String username, String token, Map<String, String> context) {
	}

	static class RecordingPlugin implements PublisherPlugin<Episode> {

		final List<Call> calls = new CopyOnWriteArrayList<>();

		@Override
		public String name() {
			return PLUGIN_NAME;
		}

		@Override
		public void publish(PublishContext<Episode> publishContext) {
			var authentication = SecurityContextHolder.getContext().getAuthentication();
			var token = authentication.getDetails() instanceof Map<?, ?> map ? (String) map.get(TOKEN_KEY) : null;
			this.calls.add(new Call(authentication.getName(), token, Map.copyOf(publishContext.context())));
			publishContext.success(PLUGIN_NAME, URI.create("https://example.com/published"));
		}

		@Override
		public boolean unpublish(UnpublishContext<Episode> uc) {
			return true;
		}

	}

	@TestConfiguration
	static class Config {

		@Bean(PLUGIN_NAME)
		RecordingPlugin publishRequestHandlerTestPlugin() {
			return new RecordingPlugin();
		}

	}

}
