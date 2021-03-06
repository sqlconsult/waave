CREATE TABLE events(
    id               int(11) not null auto_increment,   -- evt_id
    region_abbr      varchar(5),
    postal_code      varchar(10),     
    e_id             varchar(50),                       -- event_identifier
    city_name        varchar(50),       
    country_name     varchar(50),
    country_abbr     varchar(50),
    region_name      varchar(50),
    start_time       datetime,
    title            varchar(255),
    venue_address    varchar(255),
    venue_id         varchar(50),
    stop_time        datetime default null,
    venue_name       varchar(255),
    location         point,                            -- event_location
    primary key (id));