package com.joshlong.mogul.api.podcasts.production;

import com.joshlong.mogul.api.utils.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.io.File;

@Component
class AudioEncoder implements Encoder {

	private final Logger log = LoggerFactory.getLogger(getClass());

	@Override
	public File encode(File input) {
		try {
			var inputAbsolutePath = input.getAbsolutePath();
			this.log.debug("absolute path of audio file to encode: {}", inputAbsolutePath);
			Assert.state(input.exists() && input.isFile(),
					"the input ['" + inputAbsolutePath + "'] must be a valid, existing file");
			var mp3Ext = "mp3";
			if (inputAbsolutePath.toLowerCase().endsWith(mp3Ext))
				return input;
			var mp3 = FileUtils.createRelativeTempFile(input, "." + mp3Ext);
			var mp3AbsolutePath = mp3.getAbsolutePath();
			this.log.debug("mp3: {}", mp3AbsolutePath);
			var exit = Runtime.getRuntime()
				.exec(new String[] { "ffmpeg", "-i", inputAbsolutePath, mp3AbsolutePath })
				.waitFor();
			Assert.state(exit == 0, "the ffmpeg command ran successfully");
			return mp3;
		} //
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

}
