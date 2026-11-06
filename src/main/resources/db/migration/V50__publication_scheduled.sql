-- every publication now goes through jobrunr, either straight away or at a time the
-- user picked. this is that time, so the client can say "pending (for <when>)". null
-- means "as soon as possible".
alter table publication
    add column scheduled timestamp null;
