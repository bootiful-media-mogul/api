package com.joshlong.mogul.api.analysis;

/**
 * an analyzable is to be analyzed. it's a modulith event, kept until it's handled, so an
 * analysis asked for just before a restart still happens.
 *
 * @param type the analyzable's type: {@code episode}, {@code post}
 */
public record AnalysisRequestedEvent(Long mogulId, String type, Long id) {
}
