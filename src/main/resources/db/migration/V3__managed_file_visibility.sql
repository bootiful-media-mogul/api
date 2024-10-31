--  support the new visibility paradigm so that some managed files can be referenceable without an access token
alter table managed_file add column visible boolean default false;