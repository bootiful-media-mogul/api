-- a clip from the browser extension has a title (the page's title, usually) as well as its text and url
alter table note add column if not exists title text null;
