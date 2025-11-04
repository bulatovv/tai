create table if not exists sessions (
    player text,
    session_start timestamp_s,
    session_end timestamp_s,
    primary key (player, session_start)
);