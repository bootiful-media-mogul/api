alter table podcast_episode_segment
    add column transcribable boolean default true;

alter table settings
    rename column mogul_id to mogul;

alter table managed_file
    rename column mogul_id to mogul;

alter table managed_file_deletion_request
    rename column mogul_id to mogul;

alter table podcast_episode_segment
    rename column podcast_episode_id to podcast_episode;

alter table podcast_episode_segment
    rename column segment_audio_managed_file_id to segment_audio_managed_file;

alter table podcast
    rename column mogul_id to mogul;

alter table podcast_episode_segment
    rename column produced_segment_audio_managed_file_id to produced_segment_audio_managed_file;

alter table publication
    rename column mogul_id to mogul;