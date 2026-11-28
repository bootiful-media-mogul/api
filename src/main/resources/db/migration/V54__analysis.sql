-- what the model found most compelling in a post's or an episode's text. one row per
-- analyzed thing (payload, payload_class, as notes and publications have it): analyzing
-- it again replaces the row. the highlights are json, a list of {kind, quote, reason, score}.
create table if not exists analysis
(
    id            serial primary key,
    mogul_id      bigint    not null references mogul (id),
    created       timestamp not null default now(),
    payload       text      not null,
    payload_class text      not null,
    highlights    text      not null
);

create unique index if not exists analysis_payload_index on analysis (mogul_id, payload, payload_class);
