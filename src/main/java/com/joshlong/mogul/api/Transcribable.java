package com.joshlong.mogul.api;

import org.springframework.core.io.Resource;

import java.io.Serializable;

public interface Transcribable {

	Serializable key();

	Resource audio(); // maybe i need an actual ManagedFile? do i want o make that leap?

}
