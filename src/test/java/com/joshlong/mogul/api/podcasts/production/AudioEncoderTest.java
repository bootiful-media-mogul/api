package com.joshlong.mogul.api.podcasts.production;

import org.junit.jupiter.api.Test;
import org.springframework.util.Assert;
import org.springframework.util.SystemPropertyUtils;

import java.io.File;

class AudioEncoderTest {

	private final AudioEncoder encoder = new AudioEncoder();

	AudioEncoderTest() throws Exception {
	}

	void transcodeWavToMp3s() throws Exception {
		var wav = new File(
				SystemPropertyUtils.resolvePlaceholders("${HOME}/Desktop/misc/sample-podcast/test.wav ".trim()));
		Assert.state(wav.exists(), "the .wav file exists");
		var output = encoder.encode(wav);
		Assert.state(output.length() < wav.length(), "the new file should be a _lot_ smaller than the original!");
		Assert.state(output.getName().endsWith(".mp3"), "the new file should be an .mp3");
		output.delete();
	}

}