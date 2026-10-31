package com.joshlong.mogul.api.podcasts;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

/**
 * what actually gets rendered: the ranges the mogul kept by hand, minus whatever the
 * automatic cuts take out of them.
 * <p>
 * the cleaned audio has every filler and stutter muted already, so shortening one means
 * keeping only the start of its now-silent stretch, and trimming a long silence means
 * keeping a little at each end. every join falls inside silence, which is what makes them
 * inaudible. the editor's preview does the same arithmetic in typescript, and the two are
 * held to the same cases by tests on both sides.
 */
public abstract class SegmentCuts {

	/**
	 * how long a pause a filler becomes: about 45% of it, but between a tenth and a third
	 * of a second, and never longer than the filler was. this is what cleanvoice does
	 * when it cuts fillers itself, measured from its own edit lists to within 20ms.
	 */
	static long pauseFor(long fillerLength) {
		var pause = Math.max(100, Math.min(350, Math.round(fillerLength * 0.45)));
		return Math.min(fillerLength, pause);
	}

	/** pieces shorter than this aren't worth a join */
	static final long SHORTEST_PIECE = 10;

	public static List<KeptRange> compute(long duration, List<KeptRange> kept, SegmentDetections detections,
			SegmentFilters filters, Set<String> restored) {
		var byHand = kept == null || kept.isEmpty() ? List.of(new KeptRange(0, duration)) : kept;
		if (detections == null || filters == null)
			return byHand;
		var removals = new ArrayList<long[]>();
		for (var d : detections.cleanvoice()) {
			var wanted = (filters.fillers() && SegmentDetections.FILLER.equals(d.kind()))
					|| (filters.stutters() && SegmentDetections.STUTTER.equals(d.kind()));
			if (!wanted || restored.contains(d.id()))
				continue;
			var pause = pauseFor(d.end() - d.start());
			removals.add(new long[] { d.start() + pause, d.end() });
		}
		if (filters.silences())
			for (var d : detections.silences()) {
				if (d.end() - d.start() <= filters.silenceLongerThan() || restored.contains(d.id()))
					continue;
				var half = filters.silenceLeave() / 2;
				removals.add(new long[] { d.start() + half, d.end() - half });
			}
		// overlapping removals (a filler inside a long silence) merge into one
		removals.removeIf(r -> r[1] <= r[0]);
		removals.sort(Comparator.comparingLong(r -> r[0]));
		var merged = new ArrayList<long[]>();
		for (var r : removals) {
			if (!merged.isEmpty() && r[0] <= merged.getLast()[1])
				merged.getLast()[1] = Math.max(merged.getLast()[1], r[1]);
			else
				merged.add(new long[] { r[0], r[1] });
		}
		var result = new ArrayList<KeptRange>();
		for (var range : byHand) {
			var from = range.start();
			for (var r : merged) {
				if (r[1] <= from || r[0] >= range.end())
					continue;
				if (r[0] - from >= SHORTEST_PIECE)
					result.add(new KeptRange(from, r[0]));
				from = Math.max(from, r[1]);
			}
			if (range.end() - from >= SHORTEST_PIECE)
				result.add(new KeptRange(from, range.end()));
		}
		// nothing left would render as "keep everything", which is the opposite
		return result.isEmpty() ? byHand : result;
	}

}
