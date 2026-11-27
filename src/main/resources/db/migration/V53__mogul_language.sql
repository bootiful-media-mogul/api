-- the language the mogul's system is written in, as a BCP 47 tag (`es`, `fr`, `pt-BR`).
-- the client reports it, and every prompt we send to the model asks for answers in it.
--
-- nullable on purpose. a mogul who has never chosen one gets the model's default, which
-- is exactly the behaviour they have today.
alter table mogul
    add column if not exists language text;
