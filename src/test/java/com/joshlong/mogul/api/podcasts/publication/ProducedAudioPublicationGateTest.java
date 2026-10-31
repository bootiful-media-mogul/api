package com.joshlong.mogul.api.podcasts.publication;

import com.joshlong.mogul.api.PublisherPlugin;
import com.joshlong.mogul.api.podcasts.Episode;
import com.joshlong.mogul.api.podcasts.PodcastEpisodeSegmentCleanedEvent;
import com.joshlong.mogul.api.podcasts.PodcastEpisodeSegmentCleaningFailedEvent;
import com.joshlong.mogul.api.podcasts.PodcastService;
import com.joshlong.mogul.api.podcasts.production.PodcastEpisodeRenderFinishedEvent;
import com.joshlong.mogul.api.podcasts.production.PodcastProducer;
import com.joshlong.mogul.api.publications.PublicationService;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationEventPublisher;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class ProducedAudioPublicationGateTest {

	private final PodcastProducer podcastProducer = mock(PodcastProducer.class);

	private final PublicationService publicationService = mock(PublicationService.class);

	private final PodcastService podcastService = mock(PodcastService.class);

	private final ApplicationEventPublisher publisher = mock(ApplicationEventPublisher.class);

	private final ProducedAudioPublicationGate gate = new ProducedAudioPublicationGate(this.podcastProducer,
			this.publicationService, this.podcastService, this.publisher);

	private static Episode episode(boolean stale) {
		var assetsUpdated = new Date();
		var audioUpdated = stale ? new Date(assetsUpdated.getTime() - 1_000)
				: new Date(assetsUpdated.getTime() + 1_000);
		return new Episode(42L, 1L, "a title", "a description", new Date(), null, null, null, true, audioUpdated,
				assetsUpdated);
	}

	private PublicationService.PublicationAttempt<Episode> attempt(long publicationId, Episode episode,
			boolean requiresProducedAudio) {
		var plugin = new PodcastEpisodePublisherPlugin() {

			@Override
			public String name() {
				return "aPlugin";
			}

			@Override
			public boolean requiresProducedAudio() {
				return requiresProducedAudio;
			}

			@Override
			public void publish(PublishContext<Episode> publishContext) {
			}

			@Override
			public boolean unpublish(UnpublishContext<Episode> uc) {
				return true;
			}
		};
		return new PublicationService.PublicationAttempt<>(publicationId, 1L, plugin,
				PublisherPlugin.PublishContext.of(1L, episode, Map.of()));
	}

	@Test
	void aPluginThatDoesNotNeedTheAudioIsNotHeldUpForIt() {
		assertThat(this.gate.defer(this.attempt(1L, episode(true), false), () -> {
		})).isFalse();
		verifyNoInteractions(this.podcastProducer);
	}

	@Test
	void anEpisodeWhoseAudioIsCurrentIsNotRenderedAgain() {
		assertThat(this.gate.defer(this.attempt(1L, episode(false), true), () -> {
		})).isFalse();
		verifyNoInteractions(this.podcastProducer);
	}

	@Test
	void aStaleEpisodeIsRenderedAndThePublicationWaitsForIt() {
		var resumed = new AtomicInteger();
		var episode = episode(true);

		assertThat(this.gate.defer(this.attempt(1L, episode, true), resumed::incrementAndGet)).isTrue();
		verify(this.podcastProducer).produce(episode);
		assertThat(resumed).hasValue(0);

		this.gate.onRenderFinished(new PodcastEpisodeRenderFinishedEvent(42L, true, null));
		assertThat(resumed).hasValue(1);
	}

	@Test
	void oneRenderServesEveryPublicationWaitingOnIt() {
		var resumed = new AtomicInteger();
		var episode = episode(true);
		this.gate.defer(this.attempt(1L, episode, true), resumed::incrementAndGet);
		this.gate.defer(this.attempt(2L, episode, true), resumed::incrementAndGet);

		// a second render would be writing over the first one's output while the first
		// one's publication is reading it.
		verify(this.podcastProducer, times(1)).produce(episode);

		this.gate.onRenderFinished(new PodcastEpisodeRenderFinishedEvent(42L, true, null));
		assertThat(resumed).hasValue(2);
	}

	@Test
	void theSameRenderAnnouncedTwiceDoesNotPublishTwice() {
		var resumed = new AtomicInteger();
		this.gate.defer(this.attempt(1L, episode(true), true), resumed::incrementAndGet);

		this.gate.onRenderFinished(new PodcastEpisodeRenderFinishedEvent(42L, true, null));
		this.gate.onRenderFinished(new PodcastEpisodeRenderFinishedEvent(42L, true, null));

		assertThat(resumed).hasValue(1);
	}

	@Test
	void aFailedRenderAbandonsThePublicationRatherThanPublishingTheLastOne() {
		var resumed = new AtomicInteger();
		var attempt = this.attempt(1L, episode(true), true);
		this.gate.defer(attempt, resumed::incrementAndGet);

		this.gate.onRenderFinished(new PodcastEpisodeRenderFinishedEvent(42L, false, "ffmpeg exited with 1"));

		assertThat(resumed).hasValue(0);
		verify(this.publicationService).failPublication(eq(attempt), contains("ffmpeg exited with 1"));
	}

	@Test
	void aRenderThatCouldNotEvenBeAskedForDoesNotStrandThePublication() {
		var resumed = new AtomicInteger();
		var attempt = this.attempt(1L, episode(true), true);
		doThrow(new IllegalStateException("rabbit is down")).when(this.podcastProducer).produce(any());

		// still deferred: this attempt is the gate's now, and handing it back would
		// publish the episode with the audio of whatever was rendered last.
		assertThat(this.gate.defer(attempt, resumed::incrementAndGet)).isTrue();
		assertThat(resumed).hasValue(0);
		verify(this.publicationService).failPublication(eq(attempt), contains("rabbit is down"));
	}

	@Test
	void anAbandonedPublicationIsNotAlsoResumedWhenALaterRenderLands() {
		var resumed = new AtomicInteger();
		var attempt = this.attempt(1L, episode(true), true);
		doThrow(new IllegalStateException("rabbit is down")).when(this.podcastProducer).produce(any());
		this.gate.defer(attempt, resumed::incrementAndGet);

		this.gate.onRenderFinished(new PodcastEpisodeRenderFinishedEvent(42L, true, null));

		assertThat(resumed).hasValue(0);
		verify(this.publicationService, times(1)).failPublication(any(), anyString());
	}

	@Test
	void anEpisodeStillBeingCleanedWaitsWithoutRendering() {
		when(this.podcastService.isPodcastEpisodeCleaning(42L)).thenReturn(true);
		var resumed = new AtomicInteger();

		assertThat(this.gate.defer(this.attempt(1L, episode(true), true), resumed::incrementAndGet)).isTrue();

		// rendering now would render the upload, not the cleaned audio
		verifyNoInteractions(this.podcastProducer);
		verify(this.publisher).publishEvent(new PodcastEpisodePublicationWaitingForCleaningEvent(1L, 42L));
		assertThat(resumed).hasValue(0);
	}

	@Test
	void theLastSegmentCleanedRendersAndThenPublishes() {
		when(this.podcastService.isPodcastEpisodeCleaning(42L)).thenReturn(true);
		var resumed = new AtomicInteger();
		this.gate.defer(this.attempt(1L, episode(true), true), resumed::incrementAndGet);

		// cleaning re-rendered the segments, so the episode is stale now
		var cleanedEpisode = episode(true);
		when(this.podcastService.isPodcastEpisodeCleaning(42L)).thenReturn(false);
		when(this.podcastService.getPodcastEpisodeById(42L)).thenReturn(cleanedEpisode);
		this.gate.onSegmentCleaned(new PodcastEpisodeSegmentCleanedEvent(1L, 42L, 7L));

		verify(this.podcastProducer).produce(cleanedEpisode);
		assertThat(resumed).hasValue(0);
		this.gate.onRenderFinished(new PodcastEpisodeRenderFinishedEvent(42L, true, null));
		assertThat(resumed).hasValue(1);
	}

	@Test
	void oneSegmentCleanedIsNotEnoughWhileAnotherIsStillGoing() {
		when(this.podcastService.isPodcastEpisodeCleaning(42L)).thenReturn(true);
		var resumed = new AtomicInteger();
		this.gate.defer(this.attempt(1L, episode(true), true), resumed::incrementAndGet);

		this.gate.onSegmentCleaned(new PodcastEpisodeSegmentCleanedEvent(1L, 42L, 7L));

		verifyNoInteractions(this.podcastProducer);
		assertThat(resumed).hasValue(0);
	}

	@Test
	void aFailedCleaningDoesNotHoldPublishingBack() {
		when(this.podcastService.isPodcastEpisodeCleaning(42L)).thenReturn(true);
		var resumed = new AtomicInteger();
		this.gate.defer(this.attempt(1L, episode(true), true), resumed::incrementAndGet);

		// the segment fell back to its upload, which was already rendered
		when(this.podcastService.isPodcastEpisodeCleaning(42L)).thenReturn(false);
		when(this.podcastService.getPodcastEpisodeById(42L)).thenReturn(episode(false));
		this.gate.onSegmentCleaningFailed(new PodcastEpisodeSegmentCleaningFailedEvent(1L, 42L, 7L, "nope", false));

		verifyNoInteractions(this.podcastProducer);
		assertThat(resumed).hasValue(1);
	}

}
