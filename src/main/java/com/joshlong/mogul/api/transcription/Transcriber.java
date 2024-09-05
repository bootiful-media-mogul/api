package com.joshlong.mogul.api.transcription;

import org.springframework.core.io.Resource;

/**
 * given an audio file, return a textual transcription of that audio file. simple? surely.
 */
public interface Transcriber {

	String transcribe(Resource audio);

}
