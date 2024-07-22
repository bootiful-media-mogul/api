package com.joshlong.mogul.api.migration;

import com.joshlong.mogul.api.managedfiles.ManagedFile;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.graphql.client.GraphQlClient;
import org.springframework.jdbc.core.simple.JdbcClient;

import java.util.Date;
import java.util.Map;
import java.util.Objects;

class NewApiClient {

	private final GraphQlClient client;

	private final JdbcClient destinationJdbcClient;

	NewApiClient(GraphQlClient client, JdbcClient destinationJdbcClient) {
		this.client = client;
		this.destinationJdbcClient = destinationJdbcClient;
	}

	void addSegment(Long episodeId) {
		var mutation = """
				  mutation AddPodcastEpisodeSegment($episodeId: ID  ){
				            addPodcastEpisodeSegment(episodeId:$episodeId  )
				          }
				""";
		client.document(mutation)
			.variable("episodeId", episodeId)
			.executeSync()
			.field("addPodcastEpisodeSegment")
			.toEntity(Void.class);
	}

	Episode podcastEpisodeById(Long episodeId) {
		var q = """
					query PodcastEpisodeById($episodeId: ID  ){
					podcastEpisodeById(episodeId:$episodeId  )
					}
				""";
		return client.document(q).variable("episodeId", episodeId).executeSync().toEntity(Episode.class);
	}

	Long createPodcastEpisodeDraft(Long podcastId, String title, String description, Date date) {
		var gql = """
				mutation CreatePodcastEpisodeDraft ($podcast: ID, $title: String, $description: String ){
				      createPodcastEpisodeDraft( podcastId: $podcast, title: $title, description: $description) {
				           id
				      }
				     }
				""";

		var id = Long.parseLong((String) Objects.requireNonNull(client.document(gql)
			.variable("podcast", podcastId)
			.variable("title", title)
			.variable("description", description)
			.executeSync()
			.field("createPodcastEpisodeDraft")
			.toEntity(new ParameterizedTypeReference<Map<String, Object>>() {
			})).get("id"));

		this.destinationJdbcClient.sql("update podcast_episode set created = ? where id = ?")
			.params(date, podcastId)
			.update();
		return id;
	}

	Long getPodcastId() {
		var httpRequestDocument = """
				query {
					podcasts { id }
				}
				""";
		return Long.parseLong((String) this.client.document(httpRequestDocument)
			.retrieveSync("podcasts")
			.toEntityList(new ParameterizedTypeReference<Map<String, Object>>() {
			})
			.getFirst()
			.get("id"));
	}

	record Episode(String[] availablePlugins, Long id, String title, String description, boolean valid,
			ManagedFile graphic, boolean complete, float created, Segment[] segments) {
	}

	record Segment() {
	}

}
