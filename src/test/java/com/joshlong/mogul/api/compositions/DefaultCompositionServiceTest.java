package com.joshlong.mogul.api.compositions;

import com.joshlong.mogul.api.managedfiles.ManagedFileService;
import com.joshlong.mogul.api.mogul.MogulService;
import com.joshlong.mogul.api.podcasts.PodcastService;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.provisioning.InMemoryUserDetailsManager;
import org.springframework.security.test.context.support.WithUserDetails;
import org.springframework.transaction.support.TransactionTemplate;

import static com.joshlong.mogul.api.compositions.TestSecurityConfiguration.ONE;
import static org.junit.jupiter.api.Assertions.*;

@TestConfiguration
class TestSecurityConfiguration {

	static final String ONE = "joshlong";

	// static final String ONE = "google-oauth2|107746898487618710317";

	// static final String TWO = "joshlong";

	@Bean
	UserDetailsService userDetailsService() {
		var user1 = User.withUsername(ONE).password("pw").roles("USER").build();
		return new InMemoryUserDetailsManager(user1);
	}

}

@Disabled
@Import(TestSecurityConfiguration.class)
@SpringBootTest
class DefaultCompositionServiceTest {

	@BeforeAll
	static void reset(@Autowired JdbcClient db) {
		db.sql("delete from composition_attachment ").update();
		db.sql("delete from composition").update();
	}

	@Test
	@WithUserDetails(ONE)
	void composeAndAttach(@Autowired CompositionService compositionService, @Autowired PodcastService podcastService,
			@Autowired ManagedFileService managedFileService, @Autowired MogulService mogulService,
			@Autowired TransactionTemplate transactionTemplate) {
		// todo login
		var mogulId = transactionTemplate.execute(status -> {
			var login = mogulService.login(ONE, "123", "Josh", "Long");
			assertNotNull(login, "the login should not be null");
			// we should have at least one mogul at this point.
			return login.id();
		});

		var podcast = podcastService.createPodcast(mogulId, "the simplest podcast ever");
		var episode = podcastService.createPodcastEpisodeDraft(mogulId, podcast.id(), "the title", "the description");
		assertEquals(episode.id(), episode.compositionKey());
		var descriptionComp = podcastService.getPodcastEpisodeDescriptionComposition(episode.id());
		var titleComp = podcastService.getPodcastEpisodeTitleComposition(episode.id());
		assertNotNull(descriptionComp, "the composition for the description must be non-null");
		assertNotNull(titleComp, "the composition for the title must be non-null");

		assertTrue(descriptionComp.attachments().isEmpty(), "there should be no attachments for the description, yet");

		var mfForAttachment = managedFileService.createManagedFile(mogulId, "bucket", "folder", "filename", 10L,
				MediaType.IMAGE_JPEG, true);
		var attachment = compositionService.attach(descriptionComp.id(),
				"this is the nicest image that's ever been attached, ever", mfForAttachment.id());
		assertNotNull(attachment, "the attachment should not be null");

		descriptionComp = podcastService.getPodcastEpisodeDescriptionComposition(episode.id());
		assertEquals(1, descriptionComp.attachments().size(), "there should be one attachment for the title");

	}

}