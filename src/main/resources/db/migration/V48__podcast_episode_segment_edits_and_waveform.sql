-- the segment editor keeps the uploaded audio untouched and records what to keep of it.
-- the produced audio is rendered from the two. null means the segment hasn't been edited.
-- the shape is {"kept": [[startMs, endMs], ...]}, in the order the ranges play.
alter table podcast_episode_segment add column if not exists edits jsonb;

-- the waveform the editor draws, computed from the uploaded audio by the processors.
-- segments created before this have none until the editor first asks for one.
alter table podcast_episode_segment
    add column if not exists waveform_managed_file_id bigint references managed_file (id);

create index if not exists podcast_episode_segment_waveform_managed_file_index
    on podcast_episode_segment (waveform_managed_file_id);
