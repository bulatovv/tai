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
