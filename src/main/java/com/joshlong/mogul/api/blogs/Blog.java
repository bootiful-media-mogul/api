package com.joshlong.mogul.api.blogs;

import com.joshlong.mogul.api.managedfiles.ManagedFile;

import java.util.Collection;
import java.util.Date;
import java.util.Set;

public record Blog(Long mogulId, String title, Date created, Collection<Post> posts) {
}
