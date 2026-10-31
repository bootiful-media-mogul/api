package com.joshlong.mogul.api.cleanvoice;

import com.joshlong.mogul.api.managedfiles.ManagedFile;

import java.util.Map;

/**
 * cleans audio with the mogul's own cleanvoice account: background noise removed,
 * loudness normalized, music kept, and fillers, stutters and dead air muted rather than
 * cut, so the cleaned audio lines up with the original to the millisecond. it takes a
 * while -- about ten minutes for forty-five of audio -- and ends with an
 * {@link AudioCleanedEvent} or an {@link AudioCleaningFailedEvent} carrying
 * {@code context}.
 */
public interface AudioCleaning {

	void clean(Long mogulId, ManagedFile input, Map<String, Object> context);

}
