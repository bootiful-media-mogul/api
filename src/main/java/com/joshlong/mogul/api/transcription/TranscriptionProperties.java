package com.joshlong.mogul.api.transcription;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.io.File;

@ConfigurationProperties(prefix = "mogul.transcriptions")
record TranscriptionProperties(File root) {
}
