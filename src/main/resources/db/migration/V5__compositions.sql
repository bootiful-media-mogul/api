-- support the basic schema around blogs


create table composition
(
    id       serial primary key not null,
    created  timestamp          not null default now(),
    field    text               not null,
    payload       text not null,
    payload_class text not null,
    unique (payload_class, payload, field)
);

create table composition_attachment
(
    id      serial primary key not null,
    composition_id  bigint references composition (id),
    managed_file_id bigint references managed_file (id),
    created         timestamp          not null default now(),
    caption text               not null default ''
);
