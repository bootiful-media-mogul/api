package com.joshlong.mogul.api.podcasts.production;

import com.joshlong.mogul.api.managedfiles.ManagedFileService;
import com.joshlong.mogul.api.managedfiles.CommonMediaTypes;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.http.MediaType;
import org.springframework.integration.core.GenericHandler;
import org.springframework.integration.dsl.DirectChannelSpec;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.MessageChannels;
import org.springframework.messaging.MessageChannel;
import org.springframework.util.FileCopyUtils;

import java.io.*;
import java.util.function.Function;

/**
 * we want a way to take any asset - be it a {@literal .wav} or a {@literal .png} - and
 * turn it into a {@literal  .mp3} and a 1MB-or-less {@literal .jpg} respectively.
 */
@Configuration
class MediaNormalizationIntegration {

	public static final String MEDIA_NORMALIZATION_FLOW = "mediaNormalizationFlow";

	static final String MEDIA_NORMALIZATION_FLOW_CHANNEL = MEDIA_NORMALIZATION_FLOW + "Requests";

	@Bean(MEDIA_NORMALIZATION_FLOW_CHANNEL)
	DirectChannelSpec mediaNormalizationFlowRequests() {
		return MessageChannels.direct();
	}

	@Bean(MEDIA_NORMALIZATION_FLOW)
	IntegrationFlow mediaNormalizationFlow(@Qualifier(MEDIA_NORMALIZATION_FLOW_CHANNEL) MessageChannel requests,
			ImageEncoder imageEncoder, AudioEncoder audioEncoder, ManagedFileService managedFileService) {
		var imgMediaType = MediaType.parseMediaType("image/*");
		return IntegrationFlow//
			.from(requests)//
			.handle((GenericHandler<MediaNormalizationIntegrationRequest>) (io, headers) -> {
				var input = io.input();
				var output = io.output();
				var isImage = imgMediaType.isCompatibleWith(MediaType.parseMediaType(input.contentType()));
				var ext = isImage ? CommonMediaTypes.JPG : CommonMediaTypes.MP3;
				var encodingFunction = isImage ? (Function<File, File>) imageEncoder::encode
						: (Function<File, File>) audioEncoder::encode;
				var localFile = input.uniqueLocalFile();
				var resource = managedFileService.read(input.id());
				try (var i = new BufferedInputStream(resource.getInputStream());
						var o = new BufferedOutputStream(new FileOutputStream(localFile))) {
					FileCopyUtils.copy(i, o);
				} ///
				catch (IOException e) {
					throw new RuntimeException(e);
				}
				var newFile = encodingFunction.apply(localFile);
				managedFileService.write(output.id(), output.filename(), ext, new FileSystemResource(newFile));
				return new MediaNormalizationIntegrationResponse(input, output);
			})
			.get();
	}

}
