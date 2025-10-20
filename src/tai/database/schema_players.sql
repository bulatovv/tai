create table if not exists players (
    id integer,
    login text not null,
    lastlogin timestamp_s,
    regdate timestamp_s,
    moder integer,
    mute integer,
    verify integer,
    verify_text text,
    warn struct(admin text, bantime timestamp_s, reason text)[],
    snapshot_time timestamp_s,
    primary key (id, snapshot_time)
);

ALTER TABLE players ADD COLUMN IF NOT EXISTS bonuspoints UINTEGER;
ALTER TABLE players ADD COLUMN IF NOT EXISTS premium BOOLEAN;
ALTER TABLE players ADD COLUMN IF NOT EXISTS premium_expdate TIMESTAMP_S;
ALTER TABLE players ADD COLUMN IF NOT EXISTS chase_rating INTEGER;
