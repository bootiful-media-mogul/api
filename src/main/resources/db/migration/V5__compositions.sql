-- support the basic schema around blogs


create table composition
(
    -- all the following three fields are the usual things
    id            serial primary key not null,
    mogul_id      bigint             not null references mogul (id),
    created       timestamp          not null default now(),

    payload_class text               not null, -- is this a blog? a podcast? what?
    payload       text               not null, -- this is meant to be the id of the thing we're editing written as JSON
    field         text               not null, -- are we editing the description, the subject, the article, the summary, or whatever? 
    unique (mogul_id, payload, payload_class, field)

);

create table composition_attachment
(
    composition_id  bigint references composition (id),
    managed_file_id bigint references managed_file (id),
    id              serial primary key not null,
    created         timestamp          not null default now(),
    key             text               not null
);
 