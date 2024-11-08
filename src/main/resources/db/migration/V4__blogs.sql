-- support the basic schema around blogs

create table blog
(

    id          serial primary key not null,
    mogul_id    bigint             not null references mogul (id),
    title       text               not null,
    description text               not null,
    created     timestamp          not null default now(),
    unique (mogul_id, title)

);

create table blog_post
(
    blog_id  bigint references blog (id),
    id       serial primary key not null,
    created  timestamp          not null default now(),
    title    text               not null,
    content    text               not null,
    summary  text               not null default '',
    complete bool               not null default false,
    tags     text[]             null
);