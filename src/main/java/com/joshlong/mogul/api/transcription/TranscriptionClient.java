package com.joshlong.mogul.api.transcription;

import org.springframework.core.io.Resource;

interface TranscriptionClient {

	String transcribe(Resource audio);

}
