package com.joshlong.mogul.api.cleanvoice;

import java.util.Map;

/**
 * cleanvoice has finished. both urls are cleanvoice's own and expire, so whatever wants
 * the files has to fetch them soon.
 *
 * @param audioUrl the cleaned audio
 * @param labelsUrl what it muted, as audacity labels: {@code start\tend\tkind}, seconds
 */
public record AudioCleanedEvent(Long mogulId, String audioUrl, String labelsUrl, Map<String, Object> context) {

}
