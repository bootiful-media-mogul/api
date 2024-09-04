package com.joshlong.mogul.api.podcasts;

import org.springframework.aot.hint.annotation.RegisterReflectionForBinding;
import org.springframework.context.annotation.Configuration;

@Configuration
@RegisterReflectionForBinding({ Podcast.class, Episode.class, Segment.class })
class PodcastConfiguration {

}
