package com.joshlong.mogul.api.podcasts.publication;

import com.joshlong.mogul.api.Publication;
import com.joshlong.mogul.api.PublisherPlugin;
import com.joshlong.mogul.api.managedfiles.CommonMediaTypes;
import com.joshlong.mogul.api.managedfiles.ManagedFileService;
import com.joshlong.mogul.api.podcasts.Episode;
import com.joshlong.mogul.api.utils.FileUtils;
import com.joshlong.podbean.EpisodeStatus;
import com.joshlong.podbean.EpisodeType;
import com.joshlong.podbean.PodbeanClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.core.io.Resource;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import org.springframework.util.FileCopyUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

@Component(PodbeanPodcastEpisodePublisherPlugin.PLUGIN_NAME)
class PodbeanPodcastEpisodePublisherPlugin implements PodcastEpisodePublisherPlugin, BeanNameAware {

	private final Logger log = LoggerFactory.getLogger(getClass());

	/**
	 * well known values written to the context after publication.
	 */
	public static final String CONTEXT_PODBEAN_PODCAST_ID = "podbeanPodcastId";

	public static final String CONTEXT_PODBEAN_EPISODE_ID = "podbeanEpisodeId";

	public static final String CONTEXT_PODBEAN_EPISODE_PUBLISH_DATE_IN_MILLISECONDS = "contextPodbeanEpisodePublishDateInMilliseconds";

	public static final String PLUGIN_NAME = "podbean";

	private final AtomicReference<String> beanName = new AtomicReference<>();

	private final ManagedFileService managedFileService;

	private final PodbeanClient podbeanClient;

	PodbeanPodcastEpisodePublisherPlugin(ManagedFileService managedFileService, PodbeanClient podbeanClient) {
		this.managedFileService = managedFileService;
		this.podbeanClient = podbeanClient;
	}

	@Override
	public String name() {
		return this.beanName.get();
	}

	@Override
	public void setBeanName(@NonNull String name) {
		this.beanName.set(name);
	}

	@Override
	public boolean canPublish(Map<String, String> context, Episode payload) {
		return this.isConfigurationValid(context) && payload != null && payload.complete();
	}

	@Override
	public Set<String> getRequiredSettingKeys() {
		return Set.of("clientId", "clientSecret");
	}

	@Override
	public void publish(Map<String, String> context, Episode payload) {
		log.debug("publishing to podbean with context [{}] and payload [{}]. produced audio is [{}]", context, payload,
				payload.producedAudio());
		// todo some sort of thread local in which to stash the context
		// to make it available to the multitenant TokenProvider

		var tempProducedAudioFile = this.download(this.managedFileService.read(payload.producedAudio().id()),
				FileUtils.tempFileWithExtension("mp3"));
		log.debug("downloaded the produced audio file for the podcast episode {}", payload.id());
		var tempGraphicFile = this.download(this.managedFileService.read(payload.producedGraphic().id()),
				FileUtils.tempFileWithExtension("jpg"));
		log.debug("downloaded the produced graphic for the episode {}", payload.id());

		var producedAudioAuthorization = this.podbeanClient.upload(CommonMediaTypes.MP3, tempProducedAudioFile);
		log.debug("got the podcast audio authorization from podbean: {}", producedAudioAuthorization);
		var producedGraphicAuthorization = this.podbeanClient.upload(CommonMediaTypes.JPG, tempGraphicFile);
		log.debug("got the podcast graphic authorization from podbean: {}", producedGraphicAuthorization);

		// this used to be DRAFT/PUBLIC, but YOLO..
		var podbeanEpisode = this.podbeanClient.publishEpisode(payload.title(), payload.description(),
				EpisodeStatus.PUBLISH, EpisodeType.PUBLIC, producedAudioAuthorization.getFileKey(),
				producedGraphicAuthorization.getFileKey());
		log.debug("published the episode to podbean {}", podbeanEpisode.toString());

		var permalinkUrl = podbeanEpisode.getPermalinkUrl();
		if (permalinkUrl != null) {
			context.put(PublisherPlugin.CONTEXT_URL, permalinkUrl.toString());
			log.debug("got the published episode's (Episode#{}) podbean url: {}", payload.id(), permalinkUrl);
		} //
		else {
			log.debug("the published episode's (Episode#{}) podbean url is null", payload.id());
		}
		context.put(CONTEXT_PODBEAN_PODCAST_ID, podbeanEpisode.getPodcastId());
		context.put(CONTEXT_PODBEAN_EPISODE_ID, podbeanEpisode.getId());
		context.put(CONTEXT_PODBEAN_EPISODE_PUBLISH_DATE_IN_MILLISECONDS,
				Long.toString(podbeanEpisode.getPublishTime().getTime()));
		log.debug("published episode to podbean: [{}]", podbeanEpisode);
	}

	private File download(Resource resource, File file) {
		Assert.notNull(resource, "the resource you wanted to" + " download to local file [" + file.getAbsolutePath()
				+ "] does not exist");
		try (var bin = resource.getInputStream(); var bout = new FileOutputStream(file)) {
			FileCopyUtils.copy(bin, bout);
		} //
		catch (Throwable throwable) {
			log.error("could not download the resource {} to file {} ", resource.getFilename(), file.getAbsolutePath());
			throw new RuntimeException("could not download a resource ", throwable);
		}
		return file;
	}

	@Override
	public boolean unpublish(Map<String, String> context, Publication publication) {
		var done = new AtomicBoolean(false);
		var episodeId = context.get(CONTEXT_PODBEAN_EPISODE_ID);
		this.podbeanClient//
			.getAllEpisodes() // todo this is super inefficient. is there no way to get a
								// single episode?
			.stream()
			.filter(episode -> episode.getId().equalsIgnoreCase(episodeId))
			.forEach(episode -> {
				this.podbeanClient.updateEpisode(episodeId, episode.getTitle(), episode.getContent(),
						EpisodeStatus.DRAFT, EpisodeType.PRIVATE, null, null);
				done.set(true);
				log.debug("unpublishing podbean publication for episode #{}", publication.payload());
			});
		return done.get();
	}

}
