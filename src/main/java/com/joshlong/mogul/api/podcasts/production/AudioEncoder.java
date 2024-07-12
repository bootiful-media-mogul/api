package com.joshlong.mogul.api.podcasts.production;

import com.joshlong.mogul.api.utils.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import org.springframework.util.FileCopyUtils;

import java.io.File;
import java.io.InputStreamReader;

@Component
class AudioEncoder implements Encoder, ApplicationListener<ApplicationReadyEvent> {

	private final Logger log = LoggerFactory.getLogger(getClass());

	private void version() {
		try (var inputStream = new ProcessBuilder().command("ffmpeg", "-version").start().getInputStream();
				var inputStreamReader = new InputStreamReader(inputStream)) {
			var cmdOutput = FileCopyUtils.copyToString(inputStreamReader);
			log.debug(cmdOutput);
		}
		catch (Throwable throwable) {
			log.debug("couldn't get the ffmpeg version");
		}
	}

	@Override
	public File encode(File input) {
		try {
			var inputAbsolutePath = input.getAbsolutePath();
			log.debug("absolute path of audio file to encode: {}", inputAbsolutePath);
			Assert.state(input.exists() && input.isFile(),
					"the input ['" + inputAbsolutePath + "'] must be a valid, existing file");
			var mp3Ext = "mp3";
			if (inputAbsolutePath.toLowerCase().endsWith(mp3Ext))
				return input;
			var mp3 = FileUtils.createRelativeTempFile(input, "." + mp3Ext);
			var mp3AbsolutePath = mp3.getAbsolutePath();
			log.debug("mp3: {}", mp3AbsolutePath);
			var exit = Runtime.getRuntime()
				.exec(new String[] { "ffmpeg", "-i", inputAbsolutePath, mp3AbsolutePath })
				.waitFor();
			Assert.state(exit == 0, "the ffmpeg command ran successfully");
			return mp3;
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}

	}

	@Override
	public void onApplicationEvent(ApplicationReadyEvent event) {
		this.version();
	}

}
