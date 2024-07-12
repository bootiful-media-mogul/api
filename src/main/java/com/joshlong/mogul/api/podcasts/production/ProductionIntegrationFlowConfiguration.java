package com.joshlong.mogul.api.podcasts.production;

import com.joshlong.mogul.api.ApiProperties;
import com.joshlong.mogul.api.managedfiles.ManagedFileService;
import com.joshlong.mogul.api.podcasts.PodcastService;
import com.joshlong.mogul.api.managedfiles.ManagedFile;
import com.joshlong.mogul.api.podcasts.Episode;
import com.joshlong.mogul.api.utils.IntegrationUtils;
import com.joshlong.mogul.api.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.AmqpTemplate;
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
			/*
			 * .split(new AbstractMessageSplitter() {
			 *
			 * @Override protected Object splitMessage(Message<?> message) {
			 *
			 * // todo we need to lazily produce (normalize) all the sub managed_files
			 *
			 * Assert.state(message.getPayload() instanceof Episode,
			 * "the payload must be an instance of Episode"); var source = (Episode)
			 * message.getPayload();
			 *
			 * var all = new ArrayList<MediaNormalizationIntegrationRequest>();
			 * all.add(new MediaNormalizationIntegrationRequest( source.graphic(),
			 * source.producedGraphic() )); var segments =
			 * podcastService.getEpisodeSegmentsByEpisode(source.id()); for (var s :
			 * segments) { all.add(new MediaNormalizationIntegrationRequest( s.audio(),
			 * s.producedAudio() )); }
			 *
			 * return all .stream() .map(r ->
			 * MessageBuilder.withPayload(r).setHeaderIfAbsent(episodeIdHeaderName,
			 * source.id()).build()) .toList(); } })
			 * .transform(mediaNormalizer::normalize) .aggregate(new
			 * AbstractAggregatingMessageGroupProcessor() {
			 *
			 * @Override protected Object aggregatePayloads(MessageGroup group,
			 * Map<String, Object> defaultHeaders) { var transformed =
			 * group.getMessages();//
			 * Collection<Message<MediaNormalizationIntegrationResponse>> var episodeId =
			 * new AtomicLong(0); for (var m : transformed) {
			 * Assert.state(m.getHeaders().containsKey(episodeIdHeaderName),
			 * "you must have a header called '" + episodeIdHeaderName + "' " +
			 * "so we know where to continue the processing."); episodeId.set((Long)
			 * m.getHeaders().get(episodeIdHeaderName)); var reply =
			 * ((MediaNormalizationIntegrationResponse) m.getPayload());
			 * Assert.state(reply.output().written(), "the output file [" + reply.output()
			 * + "] was not written"); Assert.state(reply.input().written(),
			 * "the input file [" + reply.input() + "] was not written"); }
			 * log.debug("finished producing episode #" + episodeId + "."); return
			 * podcastService.getEpisodeById(episodeId.get()); } }) // todo see github
			 * https://github.com/bootiful-media-mogul/scratch/issues/12
			 */
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
					if (payload instanceof String jsonString) {
						var map = JsonUtils.read(jsonString, Map.class);
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
		log.debug("got the following S3 URI from the AMQP processor: {}", s3Uri);
		var episodeIdValue = headers.get(episodeIdHeaderName);
		var episodeId = episodeIdValue instanceof String episodeIdString ? //
				Long.parseLong(episodeIdString)//
				: ((Number) episodeIdValue).longValue();
		var episode = podcastService.getEpisodeById(episodeId);
		var producedAudio = episode.producedAudio();
		podcastService.writePodcastEpisodeProducedAudio(episode.id(), producedAudio.id());
		return managedFileService.getManagedFile(producedAudio.id());
	}

}
