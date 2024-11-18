-- support the basic schema around blogs


create table composition
(
-- all the following three fields are the usual things
    id       serial primary key not null,
    mogul_id bigint             not null references mogul (id),
    created  timestamp          not null default now(),
    field    text               not null,
    key      text               not null,
    unique (mogul_id, key, field)
);

create table composition_attachment
(
    composition_id  bigint references composition (id),
    managed_file_id bigint references managed_file (id),
    id              serial primary key not null,
    created         timestamp          not null default now(),
    key             text               not null
);
