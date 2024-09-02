package com.joshlong.mogul.api.transcription;

import com.joshlong.mogul.api.managedfiles.ManagedFile;
import com.joshlong.mogul.api.managedfiles.ManagedFileService;
import com.joshlong.mogul.api.podcasts.PodcastService;
import com.joshlong.mogul.api.podcasts.Segment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.openai.OpenAiAudioTranscriptionModel;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;
import org.springframework.core.io.Resource;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.core.GenericHandler;
import org.springframework.integration.dsl.DirectChannelSpec;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.MessageChannelSpec;
import org.springframework.integration.dsl.MessageChannels;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.modulith.events.ApplicationModuleListener;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import java.io.Serializable;

/**
 * asynchronously handles requests for transcription and produces responses
 * <p>
 * todo im not sure what the response should look? a string? how will it eventually result in the string being
 * attache dto a podcast, video, etc? should we send in a callback? or have a central handler that looks up things by
 * some sort of key and theres a
 */
@Configuration
class TranscriptionConfiguration {

    private final Logger log = LoggerFactory.getLogger(getClass());

    /**
     * we have Transcribable. could we derive some sort of key that could be used to resolve the record given its class
     * and a key? so, given a Transcribable, return identity that we use as a correlation key. message comes back. we
     * have the transcription service look up a given type via a collection of registered TranscribableResolvers? eg,
     * given a key 242327 and PodcastEpisode.class, there's a TranscribableResolver whihc knows how to look up that
     * Transcribable (call PodcastService#getPodcastEpisode(key)) and then call Transcribable#onTranscription(String
     * transcript)? does this mean we'd have to have transactional logic in the entity itself tho? otherwise how would
     * the updated state hit the sql db? No, maybe each package could contribute a bean of type TranscriptResolver
     * (which would be a root type) and when the message comes back, we'd look up the TranscriptResolver by its key and
     * class type. It in turn could be a service (lets say PodcastService implements TranscriptResolver, or something).
     * So what would it need? it'd need the key for the thing that's been transcribed. the ID, if nothing else.
     * So maybe Transcribable should have a `Serialiazable key()`, as well? Like `Publishable`.
     */


    static final String TRANSCRIPTION_REQUESTS_CHANNEL = "transcriptionRequests";

    static final String TRANSCRIPTION_REPLIES_CHANNEL = "transcriptionReplies";

    // a typical transcription could take a minute or an hour. we just can't know. using spring integration here is nice
    // because we can send all these requests out to AMQP (thus making them durable) and we can serialize their processing or 
    // control the concurrency for their processing here in our spring application.
    // 

    @Bean(name = TRANSCRIPTION_REPLIES_CHANNEL)
    MessageChannelSpec<DirectChannelSpec, DirectChannel> transcriptionReplies() {
        return MessageChannels.direct();
    }

    @Bean(name = TRANSCRIPTION_REQUESTS_CHANNEL)
    MessageChannelSpec<DirectChannelSpec, DirectChannel> transcriptionRequests() {
        return MessageChannels.direct();

    }

    @Bean
    IntegrationFlow transcriptionRequestsIntegrationFlow(
            @Qualifier(TRANSCRIPTION_REPLIES_CHANNEL) MessageChannel replies,
            OpenAiAudioTranscriptionModel openAiAudioTranscriptionModel) {
        return IntegrationFlow
                .from(transcriptionRequests())//
                .transform((GenericHandler<TranscriptionRequest>) (payload, headers) -> {
                    this.log.info("got a request for [{}]", payload);
                    var transcribable = payload.transcribable();
                    var resource = transcribable.audio();
                    var reply = openAiAudioTranscriptionModel.call(resource);
                    var key = transcribable.key();
                    return new TranscriptionReply(key, reply);
                })//
                .channel(replies)
                .get();
    }


    @Bean
    IntegrationFlow transcriptionRepliesIntegrationFlow(
            ApplicationEventPublisher publisher,
            @Qualifier(TRANSCRIPTION_REPLIES_CHANNEL) MessageChannel replies) {
        return IntegrationFlow // 
                .from(replies) // 
                .handle((GenericHandler<TranscriptionReply>) (payload, headers) -> { // 
                    // todo use the spring integration event adapter?
                    var transcriptionProcessedEvent = new TranscriptionProcessedEvent(payload.key(), payload.transcript());
                    publisher.publishEvent(transcriptionProcessedEvent);
                    return null;
                })
                .get();
    }


}


// stuff in this class would eventually get moved into the podcasts package, 
// being discovered as an implementation of two root interfaces.
@Configuration
class PodcastTranscriptConfiguration {

    public static final String PREFIX = "podcast-episode-";

    @EventListener
    void onTranscriptionProcessedEvent(TranscriptionProcessedEvent tpe) throws Exception {
        var key = tpe.key(); // todo verify that this is a transcription that we're interested in and can handle
        System.out.println("got a transcription processed event: [" + tpe + "]");
    }

    // this still requires ui support (a checkbox or something)

    static class PodcastEpisodeSegmentTranscribable implements Transcribable {

        private final Segment segment;
        
        private final ManagedFileService managedFileService;

        PodcastEpisodeSegmentTranscribable(Segment segment, ManagedFileService managedFileService) {
            this.segment = segment;
            this.managedFileService = managedFileService;
        }


        @Override
        public Serializable key() {
            return PREFIX + this.segment.id();
        }

        @Override
        public Resource audio() {
            var managedFile = this.segment.producedAudio();
            Assert.state( managedFile.written() , "the managed file with id #" + managedFile.id()+
                    "hasn't been written, can't transcribe!" );
            return this.managedFileService.read(managedFile.id());
        }
    }


    @Bean
    ApplicationRunner transcriptionRequestingApplicationRunner(
            PodcastService podcastService, ManagedFileService managedFileService, TranscriptionService transcriptionService) {
        return args -> {
            // todo we need to make it so that there's a checkbox (on by default) in the UI that says 'transcribable' 
            //      (if it's not got music). allow the user to specify this. then, for each of the segments, submit them 
            //      for transcription whenever we do the check on completeness. 
            var episode = podcastService.getEpisodeSegmentById(467L);
            var transcribable = new PodcastEpisodeSegmentTranscribable(episode, managedFileService);
            transcriptionService.requestTranscription(transcribable);
        };
    }

}

@Service
class DefaultTranscriptionService implements TranscriptionService {

    private final MessageChannel requests;

    DefaultTranscriptionService(@Qualifier(TranscriptionConfiguration.TRANSCRIPTION_REQUESTS_CHANNEL) MessageChannel requests) {
        this.requests = requests;
    }

    @Override
    public void requestTranscription(Transcribable transcribable) throws Exception {
        var message = MessageBuilder
                .withPayload(new TranscriptionRequest(transcribable))
                .build();
        this.requests.send(message);
    }
}

interface TranscriptionService {

    void requestTranscription(Transcribable transcribable) throws Exception;
}


record TranscriptionReply(Serializable key, String transcript) {
}

record TranscriptionProcessedEvent(Serializable key, String transcript) {
}
 