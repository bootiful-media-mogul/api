package com.joshlong.mogul.api.podcasts.production;

import com.joshlong.mogul.api.ApiProperties;
import com.joshlong.mogul.api.managedfiles.ManagedFile;
import com.joshlong.mogul.api.managedfiles.ManagedFileService;
import com.joshlong.mogul.api.podcasts.Episode;
import com.joshlong.mogul.api.podcasts.PodcastService;
import com.joshlong.mogul.api.utils.IntegrationUtils;
import com.joshlong.mogul.api.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.aot.hint.annotation.RegisterReflectionForBinding;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.amqp.dsl.Amqp;
import org.springframework.integration.core.GenericHandler;
import org.springframework.integration.dsl.DirectChannelSpec;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.MessageChannels;
import org.springframework.integration.json.ObjectToJsonTransformer;
import org.springframework.integration.transformer.AbstractTransformer;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * we have a python production that will take the audio files we feed it and turn them
 * into a final, produced audio form. this is the integration that gets our
 * {@link com.joshlong.mogul.api.podcasts.Episode episodes} in and out of that production.
 */
@RegisterReflectionForBinding({ ProductionIntegrationFlowConfiguration.ProducerInput.class,
		ProductionIntegrationFlowConfiguration.ProducerInputSegment.class, Map.class })
@Configuration
class ProductionIntegrationFlowConfiguration {

	private final Logger log = LoggerFactory.getLogger(getClass());

	static final String PRODUCTION_FLOW_REQUESTS = "podcast-episode-production-integration-flow-configuration";

	private final ManagedFileService managedFileService;

	ProductionIntegrationFlowConfiguration(ManagedFileService managedFileService) {
		this.managedFileService = managedFileService;
	}

	@Bean(PRODUCTION_FLOW_REQUESTS)
	DirectChannelSpec requests() {
		return MessageChannels.direct();
	}

	record ProducerInputSegment(int index, String s3Uri, long crossfade) {
	}

	record ProducerInput(String uid, Long episodeId, String outputS3Uri, List<ProducerInputSegment> segments) {
	}

	@Bean
	IntegrationFlow episodeProductionIntegrationFlow(ApiProperties properties, PodcastService podcastService,
			AmqpTemplate amqpTemplate, @Qualifier(PRODUCTION_FLOW_REQUESTS) MessageChannel channel) {
		var episodeIdHeaderName = "episodeId";
		var q = properties.podcasts().production().amqp().requests();
		return IntegrationFlow//
			.from(channel)//
			.handle(IntegrationUtils.debugHandler("got invoked by the gateway"))
			// todo we need to lazily produce/normalize all the managed segments
			.transform(new AbstractTransformer() {
				@Override
				protected Object doTransform(Message<?> message) {
					Assert.state(message.getPayload() instanceof Episode, "the payload must be an instance of Episode");
					var source = (Episode) message.getPayload();
					var uid = UUID.randomUUID().toString();
					var outputS3Uri = source.producedAudio().s3Uri().toString();
					var episodeId = source.id();
					var listOfInputSegments = new ArrayList<ProducerInputSegment>();
					var segments = podcastService.getEpisodeSegmentsByEpisode(episodeId);
					for (var i = 0; i < segments.size(); i++) {
						var seg = segments.get(i);
						listOfInputSegments.add(new ProducerInputSegment(i, seg.producedAudio().s3Uri().toString(),
								seg.crossFadeDuration()));
					}
					var input = new ProducerInput(uid, episodeId, outputS3Uri, listOfInputSegments);
					return MessageBuilder//
						.withPayload(input)//
						.copyHeadersIfAbsent(message.getHeaders())//
						.setHeader(episodeIdHeaderName, source.id())//
						.build();
				}
			})
			.transform(new ObjectToJsonTransformer())
			.handle(IntegrationUtils.debugHandler("about to send the request out to AMQP"))
			.handle(Amqp.outboundGateway(amqpTemplate)//
				.routingKey(q)//
				.exchangeName(q)//
			)//
			.handle(IntegrationUtils.debugHandler("got a response from AMQP"))//
			.transform(new AbstractTransformer() {
				@Override
				protected Object doTransform(Message<?> message) {
					var payload = message.getPayload();
					log.debug("payload response [{}]  class [{}]", payload, payload.getClass().getName());
					if (payload instanceof String jsonString) {
						var map = JsonUtils.read(jsonString, Map.class);
						log.debug("got a JSON map {}", "" + map);
						Assert.state(map.containsKey("outputS3Uri"),
								"the AMQP reply must contain the header 'outputS3Uri'");
						return map.get("outputS3Uri");
					}
					return null;
				}
			})
			.handle((GenericHandler<String>) (s3Uri, headers) -> {
				var managedFile = this.doWrite(episodeIdHeaderName, podcastService, managedFileService, headers, s3Uri);//
				return MessageBuilder.withPayload(managedFile).copyHeadersIfAbsent(headers).build();
			})
			.get();
	}

	private ManagedFile doWrite(String episodeIdHeaderName, PodcastService podcastService,
			ManagedFileService managedFileService, Map<String, Object> headers, String s3Uri) {
		this.log.debug("got the following S3 URI from the AMQP processor: {}", s3Uri);
		var episodeIdValue = headers.get(episodeIdHeaderName);
		var episodeId = episodeIdValue instanceof String episodeIdString ? //
				Long.parseLong(episodeIdString)//
				: ((Number) episodeIdValue).longValue();
		var episode = podcastService.getEpisodeById(episodeId);
		var producedAudio = episode.producedAudio();
		this.log.debug("writing [{}]", episode.id());
		podcastService.writePodcastEpisodeProducedAudio(episode.id(), producedAudio.id());
		this.log.debug("wrote [{}]", episode.id());
		var mf = managedFileService.getManagedFile(producedAudio.id());
		this.log.debug("got mf: [{}]", mf);
		return mf;
	}

}
