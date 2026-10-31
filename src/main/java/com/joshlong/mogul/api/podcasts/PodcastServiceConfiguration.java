package com.joshlong.mogul.api.podcasts;

import com.joshlong.mogul.api.cleanvoice.AudioCleaning;
import com.joshlong.mogul.api.cleanvoice.CleanvoiceSettings;

import com.joshlong.mogul.api.compositions.CompositionService;
import com.joshlong.mogul.api.managedfiles.ManagedFileService;
import com.joshlong.mogul.api.media.MediaService;
import org.springframework.cache.CacheManager;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.support.TransactionTemplate;

@Configuration
class PodcastServiceConfiguration {

	@Bean
	DefaultPodcastService defaultPodcastService(CompositionService cs, MediaService mn, JdbcClient db,
			ManagedFileService mfs, ApplicationEventPublisher publisher, TransactionTemplate transactionTemplate,
			CacheManager cacheManager, AudioCleaning audioCleaning, CleanvoiceSettings cleanvoiceSettings) {
		var podcastsCache = cacheManager.getCache("podcasts");
		var podcastEpisodesCache = cacheManager.getCache("podcastEpisodes");
		return new DefaultPodcastService(cs, mn, db, mfs, publisher, podcastsCache, podcastEpisodesCache,
				transactionTemplate, audioCleaning, cleanvoiceSettings);
	}

}
