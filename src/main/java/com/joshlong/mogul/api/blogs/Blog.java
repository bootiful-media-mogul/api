package com.joshlong.mogul.api.blogs;

import com.joshlong.mogul.api.Publishable;

import java.util.Collection;
import java.util.Date;

public record Blog(Long mogulId, Long id, String title, String description, Date created, Collection<Post> posts)

{

}
