SET FOREIGN_KEY_CHECKS=0;

DROP TABLE IF EXISTS `aircrafts_data`;
CREATE TABLE aircrafts_data (
    aircraft_code character(3),
    model jsonb,
    range INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO `aircrafts_data` (`aircraft_code`, `model`, `range`) VALUES ('319', '{"en": "Airbus A319-100", "ru": "Аэробус A319-100"}', 6700);
INSERT INTO `aircrafts_data` (`aircraft_code`, `model`, `range`) VALUES ('321', '{"en": "Airbus A321-200", "ru": "Аэробус A321-200"}', 5600);
INSERT INTO `aircrafts_data` (`aircraft_code`, `model`, `range`) VALUES ('CR2', '{"en": "Bombardier CRJ-200", "ru": "Бомбардье CRJ-200"}', 2700);
INSERT INTO `aircrafts_data` (`aircraft_code`, `model`, `range`) VALUES ('320', '{"en": "Airbus A320-200", "ru": "Аэробус A320-200"}', 5700);
INSERT INTO `aircrafts_data` (`aircraft_code`, `model`, `range`) VALUES ('CN1', '{"en": "Cessna 208 Caravan", "ru": "Сессна 208 Караван"}', 1200);

DROP TABLE IF EXISTS `airports_data`;
CREATE TABLE airports_data (
    airport_code character(3),
    airport_name jsonb,
    city jsonb,
    coordinates point,
    timezone TEXT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO `airports_data` (`airport_code`, `airport_name`, `city`, `coordinates`, `timezone`) VALUES ('ESL', '{"en": "Elista Airport", "ru": "Элиста"}', '{"en": "Elista", "ru": "Элиста"}', '(44.3308982849121094,46.3739013671875)', 'Europe/Moscow');
INSERT INTO `airports_data` (`airport_code`, `airport_name`, `city`, `coordinates`, `timezone`) VALUES ('KGP', '{"en": "Kogalym International Airport", "ru": "Когалым"}', '{"en": "Kogalym", "ru": "Когалым"}', '(74.5337982177734375,62.190399169921875)', 'Asia/Yekaterinburg');
INSERT INTO `airports_data` (`airport_code`, `airport_name`, `city`, `coordinates`, `timezone`) VALUES ('UFA', '{"en": "Ufa International Airport", "ru": "Уфа"}', '{"en": "Ufa", "ru": "Уфа"}', '(55.8744010925289984,54.5574989318850001)', 'Asia/Yekaterinburg');
INSERT INTO `airports_data` (`airport_code`, `airport_name`, `city`, `coordinates`, `timezone`) VALUES ('GOJ', '{"en": "Nizhny Novgorod Strigino International Airport", "ru": "Стригино"}', '{"en": "Nizhniy Novgorod", "ru": "Нижний Новгород"}', '(43.7840003967289988,56.2300987243649999)', 'Europe/Moscow');
INSERT INTO `airports_data` (`airport_code`, `airport_name`, `city`, `coordinates`, `timezone`) VALUES ('UUS', '{"en": "Yuzhno-Sakhalinsk Airport", "ru": "Хомутово"}', '{"en": "Yuzhno-Sakhalinsk", "ru": "Южно-Сахалинск"}', '(142.718002319335938,46.8886985778808594)', 'Asia/Sakhalin');

DROP TABLE IF EXISTS `boarding_passes`;
CREATE TABLE boarding_passes (
    ticket_no character(13),
    flight_id INT,
    boarding_no INT,
    seat_no character varying(4)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO `boarding_passes` (`ticket_no`, `flight_id`, `boarding_no`, `seat_no`) VALUES ('0005433922545', 24122, 60, '15F');
INSERT INTO `boarding_passes` (`ticket_no`, `flight_id`, `boarding_no`, `seat_no`) VALUES ('0005433568978', 324, 48, '13A');
INSERT INTO `boarding_passes` (`ticket_no`, `flight_id`, `boarding_no`, `seat_no`) VALUES ('0005435213164', 30586, 61, '17E');
INSERT INTO `boarding_passes` (`ticket_no`, `flight_id`, `boarding_no`, `seat_no`) VALUES ('0005434163641', 25246, 124, '27F');
INSERT INTO `boarding_passes` (`ticket_no`, `flight_id`, `boarding_no`, `seat_no`) VALUES ('0005435126194', 31908, 23, '15C');

DROP TABLE IF EXISTS `bookings`;
CREATE TABLE bookings (
    book_ref character(6),
    book_date timestamp with time zone,
    total_amount numeric(10,2)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO `bookings` (`book_ref`, `book_date`, `total_amount`) VALUES ('190B1D', '2017-08-12 12:22:00+03', 36600);
INSERT INTO `bookings` (`book_ref`, `book_date`, `total_amount`) VALUES ('E7232E', '2017-08-03 02:12:00+03', 56000);
INSERT INTO `bookings` (`book_ref`, `book_date`, `total_amount`) VALUES ('C2A4B7', '2017-07-15 01:42:00+03', 23200);
INSERT INTO `bookings` (`book_ref`, `book_date`, `total_amount`) VALUES ('A500EF', '2017-07-08 11:00:00+03', 47600);
INSERT INTO `bookings` (`book_ref`, `book_date`, `total_amount`) VALUES ('B87E08', '2017-08-01 12:26:00+03', 70800);

DROP TABLE IF EXISTS `flights`;
CREATE TABLE flights (
    flight_id INT,
    flight_no character(6),
    scheduled_departure timestamp with time zone,
    scheduled_arrival timestamp with time zone,
    departure_airport character(3),
    arrival_airport character(3),
    status character varying(20),
    aircraft_code character(3),
    actual_departure timestamp with time zone,
    actual_arrival timestamp with time zone
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO `flights` (`flight_id`, `flight_no`, `scheduled_departure`, `scheduled_arrival`, `departure_airport`, `arrival_airport`, `status`, `aircraft_code`, `actual_departure`, `actual_arrival`) VALUES (28201, 'PG0032', '2017-08-12 09:00:00+03', '2017-08-12 10:45:00+03', 'PEZ', 'DME', 'Arrived', 'CN1', '2017-08-12 09:04:00+03', '2017-08-12 10:50:00+03');
INSERT INTO `flights` (`flight_id`, `flight_no`, `scheduled_departure`, `scheduled_arrival`, `departure_airport`, `arrival_airport`, `status`, `aircraft_code`, `actual_departure`, `actual_arrival`) VALUES (8942, 'PG0439', '2017-08-23 12:55:00+03', '2017-08-23 16:20:00+03', 'LED', 'TBW', 'Scheduled', 'CN1', '\\N', '\\N');
INSERT INTO `flights` (`flight_id`, `flight_no`, `scheduled_departure`, `scheduled_arrival`, `departure_airport`, `arrival_airport`, `status`, `aircraft_code`, `actual_departure`, `actual_arrival`) VALUES (17756, 'PG0349', '2017-08-17 07:15:00+03', '2017-08-17 09:05:00+03', 'OSW', 'HMA', 'Scheduled', 'CR2', '\\N', '\\N');
INSERT INTO `flights` (`flight_id`, `flight_no`, `scheduled_departure`, `scheduled_arrival`, `departure_airport`, `arrival_airport`, `status`, `aircraft_code`, `actual_departure`, `actual_arrival`) VALUES (11014, 'PG0588', '2017-09-03 08:50:00+03', '2017-09-03 09:15:00+03', 'SVX', 'PEE', 'Scheduled', 'SU9', '\\N', '\\N');
INSERT INTO `flights` (`flight_id`, `flight_no`, `scheduled_departure`, `scheduled_arrival`, `departure_airport`, `arrival_airport`, `status`, `aircraft_code`, `actual_departure`, `actual_arrival`) VALUES (5888, 'PG0496', '2017-08-24 12:45:00+03', '2017-08-24 14:55:00+03', 'SVO', 'JOK', 'Scheduled', 'CN1', '\\N', '\\N');

DROP TABLE IF EXISTS `seats`;
CREATE TABLE seats (
    aircraft_code character(3),
    seat_no character varying(4),
    fare_conditions character varying(10)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO `seats` (`aircraft_code`, `seat_no`, `fare_conditions`) VALUES ('SU9', '1F', 'Business');
INSERT INTO `seats` (`aircraft_code`, `seat_no`, `fare_conditions`) VALUES ('763', '28B', 'Economy');
INSERT INTO `seats` (`aircraft_code`, `seat_no`, `fare_conditions`) VALUES ('773', '41C', 'Economy');
INSERT INTO `seats` (`aircraft_code`, `seat_no`, `fare_conditions`) VALUES ('773', '14A', 'Comfort');
INSERT INTO `seats` (`aircraft_code`, `seat_no`, `fare_conditions`) VALUES ('763', '38A', 'Economy');

DROP TABLE IF EXISTS `ticket_flights`;
CREATE TABLE ticket_flights (
    ticket_no character(13),
    flight_id INT,
    fare_conditions character varying(10),
    amount numeric(10,2)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO `ticket_flights` (`ticket_no`, `flight_id`, `fare_conditions`, `amount`) VALUES ('0005435706759', 2459, 'Business', 86800);
INSERT INTO `ticket_flights` (`ticket_no`, `flight_id`, `fare_conditions`, `amount`) VALUES ('0005434879825', 14606, 'Economy', 16600);
INSERT INTO `ticket_flights` (`ticket_no`, `flight_id`, `fare_conditions`, `amount`) VALUES ('0005435718911', 15313, 'Economy', 9000);
INSERT INTO `ticket_flights` (`ticket_no`, `flight_id`, `fare_conditions`, `amount`) VALUES ('0005434908381', 25837, 'Economy', 6300);
INSERT INTO `ticket_flights` (`ticket_no`, `flight_id`, `fare_conditions`, `amount`) VALUES ('0005435824987', 1304, 'Economy', 66600);

DROP TABLE IF EXISTS `tickets`;
CREATE TABLE tickets (
    ticket_no character(13),
    book_ref character(6),
    passenger_id character varying(20)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO `tickets` (`ticket_no`, `book_ref`, `passenger_id`) VALUES ('0005432383693', '1EE239', '8882 721205');
INSERT INTO `tickets` (`ticket_no`, `book_ref`, `passenger_id`) VALUES ('0005432646309', '022379', '7401 627389');
INSERT INTO `tickets` (`ticket_no`, `book_ref`, `passenger_id`) VALUES ('0005435097436', '0ED676', '1181 314874');
INSERT INTO `tickets` (`ticket_no`, `book_ref`, `passenger_id`) VALUES ('0005433923964', 'D68325', '2076 240051');
INSERT INTO `tickets` (`ticket_no`, `book_ref`, `passenger_id`) VALUES ('0005433784739', '972937', '0556 016120');

SET FOREIGN_KEY_CHECKS=1;
