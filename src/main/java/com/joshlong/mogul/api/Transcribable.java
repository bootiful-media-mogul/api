package com.joshlong.mogul.api;

import org.springframework.core.io.Resource;

import java.io.Serializable;

/**
 * all things that can produce bytes pointing to audio can avail themselves of a
 * transcript.
 */
public interface Transcribable {

	Class<?> subject();

	Serializable transcriptionKey();

	Resource audio();

}
