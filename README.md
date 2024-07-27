Here's the grammar-checked and spell-checked version of your text:

# README

See [the project README for what we're all about](https://github.com/bootiful-media-mogul/.github/blob/main/profile/README.md).

## TODO
- Show the date and time of a given episode on the edit episode page. As it stands, there's no way to know when an episode was published.
- Build a program to import the old episodes to the new podcast system; it’s time for this thing to earn its keep and for me to get off the old SQL DB. I'm paying DigitalOcean, might as well leverage it!
- Build a transcription integration flow that sends an .mp3 to Whisper (which I need to deploy) and then returns the text from the audio (use a Spring Integration gateway perhaps?).
- Build a way to manage channels (YouTube channels) so that I can configure credentials for SpringSourceDev and CoffeeSoftware and then _import_ the videos from those channels. Then I can link those to the actual source videos. Basically, I'll have a way to upload a video, which will in turn dump it into S3 and then run a process (using JobRunr?) to strip the audio from the video and then send the audio to the transcription service. The result will be that the transcript from the video gets attached to the video. I can then edit that in an AI text composition panel.
- I should be able to get the audio from a produced podcast and then send that to the transcription service as well.
- It should be possible, therefore, to have a button to 'produce' a new blog from the transcript and then use AI tools to work on the transcript and turn it into a blog.

## Useful Queries
- Make sure we didn't inadvertently create a `managed_file` that has no link to something in the graph.

If you need any more adjustments or additions, feel free to ask!

```sql

select *
from managed_file mf
where id not in (select pid
                 from (select p.produced_graphic as pid
                       from podcast_episode p
                       union
                       select p.graphic as pid
                       from podcast_episode p
                       union
                       select pe.produced_audio as pid
                       from podcast_episode pe
                       union
                       select pes.produced_segment_audio_managed_file_id as pid
                       from podcast_episode pe,
                            podcast_episode_segment pes
                       where pe.id = pes.podcast_episode_id
                       UNION
                       select pes.segment_audio_managed_file_id as pid
                       from podcast_episode pe,
                            podcast_episode_segment pes
                       where pe.id = pes.podcast_episode_id)
                 order by pid);

```