CREATE TABLE airports(
    id                int,
    ident             varchar(20),
    airport_type      varchar(20),
    airport_name      varchar(100),
    elevation_ft      int,
    continent         varchar(10),
    iso_country       varchar(10),
    iso_region        varchar(10),
    municipality      varchar(100),
    scheduled_service varchar(10),
    icao_code         varchar(20),
    iata_code         varchar(10),
    airport_location  point
);