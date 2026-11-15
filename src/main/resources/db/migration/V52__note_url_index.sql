-- the clipper looks up a mogul's note for the page it's on every time it opens
create index if not exists note_mogul_id_url_idx on note (mogul_id, url);
