package com.joshlong.mogul.api.migration;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.stream.Collectors;

class OldApiClient {

	private final JdbcClient sourceJdbcClient;

	OldApiClient(JdbcClient sourceJdbcClient) {
		this.sourceJdbcClient = sourceJdbcClient;
	}

	record Podcast(Integer id, Date date, String description, String notes, String podbeanDraftCreated,
			String podbeanDraftPublished, String podbeanMediaUri, String podbeanPhotoUri, String s3AudioFileName,
			String s3AudioUri, String s3PhotoFileName, String s3PhotoUri, String title, String transcript, String uid,
			Collection<Media> media) {
	}

	record Media(Integer id, String description, String extension, String fileName, String href, String type) {
	}

	record PodcastMedia(Integer podcastId, Integer mediaId) {
	}

	Collection<PodcastMedia> podcastMedia() {
		return this.sourceJdbcClient.sql("select * from podcast_media")
			.query((rs, rowNum) -> new PodcastMedia(rs.getInt("podcast_id"), rs.getInt("media_id")))
			.list();
	}

	Collection<Podcast> podcasts(Collection<Media> media) {
		var mediaMap = media.stream().collect(Collectors.toMap(Media::id, media12 -> media12));
		var podcastMedia = this.podcastMedia();
		return this.sourceJdbcClient.sql("select * from podcast").query((rs, rowNum) -> {

			var podcast = new Podcast(rs.getInt("id"), rs.getDate("date"), rs.getString("description"),
					rs.getString("notes"), rs.getString("podbean_draft_created"),
					rs.getString("podbean_draft_published"), rs.getString("podbean_media_uri"),
					rs.getString("podbean_photo_uri"), rs.getString("s3_audio_file_name"), rs.getString("s3_audio_uri"),
					rs.getString("s3_photo_file_name"), rs.getString("s3_photo_uri"), rs.getString("title"),
					rs.getString("transcript"), rs.getString("uid"), new HashSet<>());

			podcastMedia.stream()
				// find the PodcastMedia tables that match this podcast
				.filter(pm -> pm.podcastId().equals(podcast.id()))
				// for each PM, find the media and it to the podcast's media collection
				.forEach(pm -> {
					var mediaId = pm.mediaId();
					if (mediaMap.containsKey(mediaId))
						podcast.media().add(mediaMap.get(mediaId));
				});

			return podcast;
		})//
			.list();
	}

	Collection<Media> media() {
		var m = this.sourceJdbcClient.sql("select * from media")
			.query((rs, rowNum) -> new Media(rs.getInt("id"), rs.getString("description"), rs.getString("extension"),
					rs.getString("file_name"), rs.getString("href"), rs.getString("type")))
			.list();

		return m;
	}

}
