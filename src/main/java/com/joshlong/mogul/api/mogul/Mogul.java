package com.joshlong.mogul.api.mogul;

import java.util.Date;

/**
 * the main tenant/user of this system.
 */
public record Mogul(Long id, String username, String email, String clientId, String givenName, String familyName ,
                    Date updated ) {
}
