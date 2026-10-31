-- cleanvoice's cleaned copy of the uploaded audio, which the produced audio is rendered
-- from once it exists. it lines up with the upload to the millisecond.
alter table podcast_episode_segment
    add column if not exists cleaned_audio_managed_file_id bigint references managed_file (id);

create index if not exists podcast_episode_segment_cleaned_audio_managed_file_index
    on podcast_episode_segment (cleaned_audio_managed_file_id);

-- the etag of the upload the cleaned audio was made from. an upload since makes it stale.
alter table podcast_episode_segment add column if not exists cleaned_from_etag text;

-- set while the segment is being cleaned and re-rendered; null otherwise. publishing an
-- episode waits for every one of its segments to be done.
alter table podcast_episode_segment add column if not exists cleaning_started timestamp;

-- why the last cleaning failed, if it did; the segment falls back to the upload.
alter table podcast_episode_segment add column if not exists cleaning_error text;

-- what cleanvoice muted and where the long silences are, in milliseconds:
-- {"cleanvoice": [{"start", "end", "kind"}], "silences": [{"start", "end"}]}
alter table podcast_episode_segment add column if not exists detections jsonb;
