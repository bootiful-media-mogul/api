package com.joshlong.mogul.api.cleanvoice;

import java.util.Optional;

/**
 * whose cleanvoice account work is done against. a request already knows: it's the
 * signed-in mogul. a background job, ten minutes later, doesn't, so it says so
 * explicitly:
 *
 * <pre>
 * var edit = CleanvoiceTenant.callAs(mogulId, () -&gt; cleanvoice.getEdit(editId));
 * </pre>
 */
public final class CleanvoiceTenant {

	private static final ScopedValue<Long> MOGUL = ScopedValue.newInstance();

	private CleanvoiceTenant() {
	}

	public static <T, X extends Throwable> T callAs(Long mogulId, ScopedValue.CallableOp<T, X> work) throws X {
		return ScopedValue.where(MOGUL, mogulId).call(work);
	}

	static Optional<Long> current() {
		return MOGUL.isBound() ? Optional.of(MOGUL.get()) : Optional.empty();
	}

}
