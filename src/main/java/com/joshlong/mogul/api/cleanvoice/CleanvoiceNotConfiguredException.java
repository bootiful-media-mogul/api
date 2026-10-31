package com.joshlong.mogul.api.cleanvoice;

/**
 * the mogul hasn't put a cleanvoice api key in their settings.
 */
public class CleanvoiceNotConfiguredException extends IllegalStateException {

	CleanvoiceNotConfiguredException(Long mogulId) {
		super("mogul #" + mogulId + " has no cleanvoice api key in their settings");
	}

}
