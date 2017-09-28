CREATE TABLE speedruncom_twitch_game (
    twitchGame VARCHAR NOT NULL PRIMARY KEY,
    game VARCHAR NULL UNIQUE
);

CREATE TABLE speedruncom_user (
    broadcaster VARCHAR NOT NULL PRIMARY KEY,
    userid VARCHAR NOT NULL
);

CREATE TABLE speedruncom_game (
    broadcaster VARCHAR NOT NULL PRIMARY KEY,
    game VARCHAR NOT NULL
);

CREATE TABLE speedruncom_level (
    broadcaster VARCHAR NOT NULL,
    game VARCHAR NOT NULL,
    level VARCHAR NOT NULL,
    PRIMARY KEY (broadcaster, game)
);

CREATE TABLE speedruncom_category (
    broadcaster VARCHAR NOT NULL,
    game VARCHAR NOT NULL,
    level VARCHAR NOT NULL,
    category VARCHAR NOT NULL,
    PRIMARY KEY (broadcaster, game, level)
);

CREATE TABLE speedruncom_variable (
    broadcaster VARCHAR NOT NULL,
    game VARCHAR NOT NULL,
    level VARCHAR NOT NULL,
    category VARCHAR NOT NULL,
    variable VARCHAR NOT NULL,
    value VARCHAR NOT NULL,
    PRIMARY KEY (broadcaster, game, level, category, variable)
);

CREATE TABLE speedruncom_game_options (
    broadcaster VARCHAR NOT NULL,
    game VARCHAR NOT NULL,
    region VARCHAR,
    platform VARCHAR,
    emulators BOOLEAN,
    PRIMARY KEY (broadcaster, game)
);
