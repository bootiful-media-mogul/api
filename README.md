# README

See [the project readme for what we're all about](https://github.com/bootiful-media-mogul/.github/blob/main/profile/README.md)


## todo
- show the date and time of a given episode on the edit episode page. as it stands there's no way to know when an episode was published.
- build a program to import the old episodes to the new podcast system; it’s time for this thing to earn its keep and for me to get off the old SQL db. im paying digitalocean, might as well leverage it!
- build a transcription integration flow that sends an .mp3 and sends it to whisper (which i need to deploy) and then returns the text from the audio (use a Spring INtegration gateway perhaps?)
- build a way to manage channels (youtube channels) so that i can configure credentials for springsourcedev and coffeesoftware and then _import_ the videos from that channel. then i can link those to the actual source videos for the video. basically ill have a way to upload  a video which wull in turn dump it into S3 and then run a process (using jobrunr?) to strip the audio from the video and then send the audio to the transcription service. th eresult will be that the transcript from the video gets attached to the video. i can then edit that in an AI text composition panel. 
- i should be able to get the audio from a produced podcast and then send that toi the transcription service as well. 
- it should be possible therefore to have a button to 'produce' a new blog from the transcript and then use AI tools to work on the transcript and turn it into a blog. 