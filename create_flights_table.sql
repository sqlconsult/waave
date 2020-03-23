create table flights (
    process_date          date,
    airport               varchar(5),
    ident                 varchar(10),
    origin                varchar(10),
    destination           varchar(10),
    schedule_time         datetime,
    aircraft_type         varchar(10),
    direction             varchar(10),    
    seats_cabin_first     int,
    seats_cabin_business  int,
    seats_cabin_coach     int,
    request_num           int,
    offset                int
)