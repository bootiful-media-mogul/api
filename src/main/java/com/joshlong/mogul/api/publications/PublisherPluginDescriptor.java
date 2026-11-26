package com.joshlong.mogul.api.publications;

/**
 * a publisher plugin as a client sees it: its name, the kind of publishable it publishes
 * (as {@code episode}, {@code post} or {@code mogulStatus}), and whether a publication
 * through it can be scheduled for later.
 */
record PublisherPluginDescriptor(String name, String publishableType, boolean schedulable) {
}
