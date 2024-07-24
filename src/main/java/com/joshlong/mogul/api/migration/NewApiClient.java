package com.joshlong.mogul.api.migration;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.core.io.Resource;
import org.springframework.graphql.client.HttpSyncGraphQlClient;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.client.MultipartBodyBuilder;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.util.Assert;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestTemplate;

import java.util.Date;
import java.util.Map;
import java.util.Objects;

class NewApiClient {

	private final Log log = LogFactory.getLog(getClass());

	private final HttpSyncGraphQlClient client;

	private final JdbcClient destinationJdbcClient;

	private final RestClient httpClient;

	private final RestTemplate restTemplate;

	NewApiClient(HttpSyncGraphQlClient client, JdbcClient destinationJdbcClient, RestClient http,
			RestTemplate restTemplate) {
		this.client = client;
		this.httpClient = http;
		this.destinationJdbcClient = destinationJdbcClient;
		this.restTemplate = restTemplate;
	}

	void writeManagedFile(Long id, Resource resource) throws Exception {
		Assert.notNull(resource, "the resource must not be null");
		Assert.notNull(id, "the ManagedFile ID must not be null");
		var headers = new HttpHeaders();
		headers.setContentType(MediaType.MULTIPART_FORM_DATA);
		var builder = new MultipartBodyBuilder();
		builder.part("file", resource);
		var build = builder.build();
		var httpEntity = new HttpEntity<>(build, headers);
		this.restTemplate.exchange("/managedfiles/{id}", HttpMethod.POST, httpEntity, String.class, id);
	}

	void addPodcastEpisodeSegment(Long episodeId) {
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
		var graphqlQueryDocument = """

				query PodcastEpisodeById($episodeId: ID  ) {

					podcastEpisodeById(id: $episodeId) {
							availablePlugins, id, title, description, valid, complete, created,
							segments  {
				   			id,
				   			name,
				   			audio {
				   				id,
				   				bucket,
				   				folder,
				   				filename,
				   				contentType,
				   				size,
				   				written
							},
				   			producedAudio {
				   				id ,
				   				bucket ,
				   				folder,
				   				filename,
				   				contentType,
				   				size,
				   				written
				   			},
				   			order ,
				   			crossFadeDuration
				   }


							graphic  {
								id ,
								bucket ,
								folder,
								filename,
								contentType,
								size,
								written
						   } ,



					}
				}
				""";
		return client.document(graphqlQueryDocument)
			.variable("episodeId", episodeId)
			.retrieveSync("podcastEpisodeById")
			.toEntity(Episode.class);
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
		var entityList = (String) this.client.document(httpRequestDocument)
			.retrieveSync("podcasts")
			.toEntityList(Map.class)
			.getFirst()
			.get("id");
		return Long.parseLong(entityList);
	}

	record Episode(String[] availablePlugins, Long id, String title, String description, boolean valid,
			ManagedFile graphic, boolean complete, float created, Segment[] segments) {
	}

	record ManagedFile(Long id, String bucket, String folder, String filename, String contentType, long size,
			boolean written) {
	}

	record Segment(Long id, String name, ManagedFile audio, ManagedFile producedAudio, int order,
			long crossFadeDuration) {
	}

}
