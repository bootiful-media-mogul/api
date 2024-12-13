package com.joshlong.mogul.api.compositions;

import com.joshlong.mogul.api.managedfiles.ManagedFileService;
import com.joshlong.mogul.api.mogul.MogulAuthenticatedEvent;
import com.joshlong.mogul.api.podcasts.PodcastService;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.provisioning.InMemoryUserDetailsManager;
import org.springframework.security.test.context.support.WithUserDetails;
import org.springframework.transaction.annotation.Transactional;

import static com.joshlong.mogul.api.compositions.TestSecurityConfiguration.USERNAME;
import static org.junit.jupiter.api.Assertions.*;

@TestConfiguration
class TestSecurityConfiguration {


	static final String USERNAME = "google-oauth2|107746898487618710317";

	@Bean
	UserDetailsService userDetailsService() {
		var user = User.withUsername(USERNAME)
				.password("pw")
				.roles("USER")
				.build();
		return new InMemoryUserDetailsManager(user);
	}
}

@Disabled
@Import(TestSecurityConfiguration.class)
@Transactional
@SpringBootTest
class DefaultCompositionServiceTest {

	private final JdbcClient db;
	private final PodcastService podcastService;
	private final CompositionService compositionService;
	private final ManagedFileService managedFileService;
	private final ApplicationEventPublisher publisher;
	
	DefaultCompositionServiceTest(
            @Autowired ApplicationEventPublisher publisher,
            @Autowired JdbcClient db,
            @Autowired CompositionService compositionService,
            @Autowired PodcastService podcastService,
            @Autowired ManagedFileService managedFileService) {
		this.db = db;
		this.podcastService = podcastService;
		this.compositionService = compositionService;
		this.managedFileService = managedFileService;
        this.publisher = publisher ;
    }

	@BeforeAll
	static void reset(@Autowired JdbcClient db) {
		db.sql("delete from composition_attachment ").update();
		db.sql("delete from composition").update();
	}

/*

	@EventListener
	void authenticationSuccessEvent(AuthenticationSuccessEvent ase) {
		this.transactionTemplate.execute(status -> {
			var authentication = (JwtAuthenticationToken) ase.getAuthentication();
			this.login(authentication);
			return null;
		});
	}
*/


	@Test
	@WithUserDetails(USERNAME)
	void composeAndAttach() {
		//todo login 
		publisher.publishEvent(new MogulAuthenticatedEvent());
		var mogulId = this.db.sql(" select id from mogul limit 1 ").query(Long.class).single();

		var podcast = this.podcastService.createPodcast(mogulId, "the simplest podcast ever");
		var episode = this.podcastService.createPodcastEpisodeDraft(mogulId, podcast.id(), "the title", 
				"the description");
		assertEquals(episode.id(), episode.compositionKey());
		var descriptionComp = this.podcastService.getPodcastEpisodeDescriptionComposition(episode.id());
		var titleComp = this.podcastService.getPodcastEpisodeTitleComposition(episode.id());
		assertNotNull(descriptionComp, "the composition for the description must be non-null");
		assertNotNull(titleComp, "the composition for the title must be non-null");

		assertTrue(descriptionComp.attachments().isEmpty(), "there should be no attachments for the description, yet");

		var mfForAttachment = this.managedFileService
				.createManagedFile(mogulId, "bucket", "folder", "filename", 10L, MediaType.IMAGE_JPEG, true);
		var attachment = this.compositionService.attach(descriptionComp.id(), "this is the nicest image that's ever been attached, ever",
				mfForAttachment.id());
		assertNotNull(attachment, "the attachment should not be null");

		descriptionComp = this.podcastService.getPodcastEpisodeDescriptionComposition(episode.id());
		assertEquals(1, descriptionComp.attachments().size(), "there should be one attachment for the title");

	}

}