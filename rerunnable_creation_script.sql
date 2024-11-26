DROP SCHEMA IF EXISTS airline_booking2 CASCADE;
CREATE SCHEMA airline_booking2;

---------------------------------------------------------------
-- Tables
---------------------------------------------------------------

CREATE TABLE airline_booking2.passenger (
    id int PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    passenger_name varchar(100) NOT NULL,
    passport_id varchar(9),
    phone varchar(15) NOT NULL,
    email varchar(200),
    address varchar(200) NOT NULL
);

CREATE TABLE airline_booking2.seat_type (
    id int PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    seat_type varchar(15) UNIQUE NOT NULL
);

CREATE TABLE airline_booking2.plane_type (
    id int PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    plane_name varchar(30) UNIQUE NOT NULL
);

CREATE TABLE airline_booking2.plane_type_seat_type (
    id int PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    plane_type_id int NOT NULL REFERENCES airline_booking2.plane_type (id),
    seat_type_id int NOT NULL REFERENCES airline_booking2.seat_type (id),
    quantity int NOT NULL
);

CREATE TABLE airline_booking2.plane (
    id int PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    plane_type_id int NOT NULL REFERENCES airline_booking2.plane_type (id)
);

CREATE TABLE airline_booking2.airport (
    id int PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    code varchar(3) NOT NULL,
    address varchar(200) NOT NULL
);

CREATE TABLE airline_booking2.overbooking_rate (
    id int PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    rate decimal(5, 2) NOT NULL UNIQUE CHECK (rate > 0)
);

CREATE TABLE airline_booking2.scheduled_flight (
    id int PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    departure_time timestamp NOT NULL,
    arrival_time timestamp NOT NULL CHECK (arrival_time > departure_time),
    plane_id int NOT NULL REFERENCES airline_booking2.plane (id),
    departure_airport_id int NOT NULL REFERENCES airline_booking2.airport (id),
    arrival_airport_id int NOT NULL REFERENCES airline_booking2.airport (id) CHECK (arrival_airport_id != departure_airport_id),
    overbooking_id int NOT NULL REFERENCES airline_booking2.overbooking_rate (id)
);

CREATE TABLE airline_booking2.reservation (
    id int PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    passenger_id int NOT NULL,
    scheduled_flight_id int NOT NULL,
    ticket_cost decimal(5, 2) NOT NULL CHECK (ticket_cost > 0),
    seat_type_id int NOT NULL,
    seat_count int NOT NULL,
    CONSTRAINT fk_passenger_id FOREIGN KEY (passenger_id) REFERENCES airline_booking2.passenger (id),
    CONSTRAINT fk_scheduled_flight_id FOREIGN KEY (scheduled_flight_id) REFERENCES airline_booking2.scheduled_flight (id),
    CONSTRAINT fk_seat_type_id FOREIGN KEY (seat_type_id) REFERENCES airline_booking2.seat_type (id)
);

CREATE TABLE airline_booking2.payment (
    id int PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    reservation_id int NOT NULL,
    amount decimal(5, 2) NOT NULL,
    CONSTRAINT fk_reservation_id FOREIGN KEY (reservation_id) REFERENCES airline_booking2.reservation (id)
);

CREATE TABLE airline_booking2.flight_history (
    id int PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    scheduled_flight_id int NOT NULL REFERENCES airline_booking2.scheduled_flight (id),
    plane_id int NOT NULL REFERENCES airline_booking2.plane (id),
    actual_departure_time timestamp,
    actual_arrival_time timestamp CHECK (actual_arrival_time > actual_departure_time)
);

CREATE TABLE airline_booking2.seat (
    id int PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    reservation_id int NOT NULL,
    printed_boarding_pass_at timestamp,
    seat_number int,
    passenger_id int,
    CONSTRAINT fk_ab_reservation_id FOREIGN KEY (reservation_id) REFERENCES airline_booking2.reservation (id)
);

CREATE TABLE airline_booking2.concession_purchase (
    id int PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    payment_id int NOT NULL,
    seat_id int NOT NULL,
    CONSTRAINT cp_payment_id FOREIGN KEY (payment_id) REFERENCES airline_booking2.payment (id),
    CONSTRAINT cp_seat_id FOREIGN KEY (seat_id) REFERENCES airline_booking2.seat (id)
);

CREATE TABLE airline_booking2.product (
    id int PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    concession_name varchar(200) UNIQUE NOT NULL,
    price decimal(5, 2) NOT NULL CHECK (price > 0)
);

CREATE TABLE airline_booking2.concession_purchase_product (
    id int PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    product_id int NOT NULL,
    concession_purchase_id int NOT NULL,
    quantity int NOT NULL,
    CONSTRAINT fk_product_id FOREIGN KEY (product_id) REFERENCES airline_booking2.product (id),
    CONSTRAINT fk_concession_purchase_id FOREIGN KEY (concession_purchase_id) REFERENCES airline_booking2.concession_purchase (id)
);

CREATE EXTENSION IF NOT EXISTS btree_gist;

ALTER TABLE airline_booking2.scheduled_flight
    ADD CONSTRAINT prevent_overlapping_flights
    EXCLUDE USING gist (plane_id WITH =, tsrange(departure_time, arrival_time, '[)') WITH &&)
WHERE (departure_time IS NOT NULL AND arrival_time IS NOT NULL);

---------------------------------------------------------------
-- Functions
---------------------------------------------------------------

-- flight performance efficiency function
-- calculates percentages based on the flights that have been canceled
CREATE OR REPLACE FUNCTION flight_performance_efficiency ()
    RETURNS TABLE (
        percent_flights_on_time decimal(10, 6),
        percent_flights_canceled decimal(10, 6)
    )
    AS $$
BEGIN
    RETURN query WITH flight_counts AS (
        SELECT
            count(*) AS total_flights,
            sum(
                CASE WHEN fh.actual_departure_time IS NULL
                    AND fh.actual_arrival_time IS NULL THEN
                    1
                ELSE
                    0
                END) AS canceled_flights,
            sum(
                CASE WHEN fh.actual_departure_time IS NOT NULL
                    AND fh.actual_arrival_time IS NOT NULL THEN
                    1
                ELSE
                    0
                END) AS on_time_flights
        FROM
            airline_booking2.flight_history fh
)
    SELECT
        (coalesce(on_time_flights, 0) * 100.0 / coalesce(total_flights, 1))::decimal(10, 6) AS percent_flights_on_time,
    (coalesce(canceled_flights, 0) * 100.0 / coalesce(total_flights, 1))::decimal(10, 6) AS percent_flights_canceled
FROM
    flight_counts;
END;
$$
LANGUAGE plpgsql;

-- Flight Estimations Query/Function
-- Calculates the expected revenue within a 10 day interval after a given startdate
CREATE OR REPLACE FUNCTION flight_revenue_estimate (startdate date)
    RETURNS TABLE (
        start_date date,
        end_date date,
        revenue decimal(10, 2)
    )
    AS $$
BEGIN
    RETURN query
    SELECT
        (
            SELECT
                min(departure_time)::date
            FROM
                airline_booking2.scheduled_flight
            WHERE
                departure_time >= flight_revenue_estimate.startdate) AS start_date,
        (startdate + interval '10 days')::date AS end_date,
        sum(p.amount)::decimal(10, 2) AS revenue
    FROM
        airline_booking2.scheduled_flight sf
        INNER JOIN airline_booking2.reservation r ON sf.id = r.scheduled_flight_id
        INNER JOIN airline_booking2.payment p ON r.id = p.reservation_id
    WHERE
        sf.departure_time >= flight_revenue_estimate.startdate
        AND sf.arrival_time < (startdate + interval '10 days')
    GROUP BY
        start_date,
        end_date;
    -- Group by to return correct aggregates
END;
$$
LANGUAGE plpgsql;

---------------------------------------------------------------
-- Procedures
---------------------------------------------------------------

-- flight continuity procedure
-- makes sure planes aren't teleporting between flights
CREATE OR REPLACE PROCEDURE flight_continuity ()
LANGUAGE plpgsql
AS $$
DECLARE
    current_row RECORD;
    -- Variable to hold each row during iteration
    last_row RECORD;
    -- Variable to store the last row
BEGIN
    FOR current_row IN
    SELECT
        id,
        departure_airport_id,
        arrival_airport_id,
        plane_id
    FROM
        airline_booking2.scheduled_flight
    ORDER BY
        plane_id ASC,
        departure_time ASC -- Ensure rows are processed in a defined order
        LOOP
            -- If last_row is not null, perform the comparison
            IF last_row IS NOT NULL THEN
                IF last_row.arrival_airport_id = current_row.departure_airport_id THEN
                    --RAISE NOTICE 'Row continuity check passed: Plane ID=%, Last Arrival=%, Current Departure=%', last_row.plane_id, last_row.arrival_airport_id, current_row.departure_airport_id;
                ELSIF last_row.plane_id != current_row.plane_id THEN
                    --RAISE NOTICE 'Row continuity check passed: new plane: Plane ID=%, Departure=%', last_row.plane_id, current_row.departure_airport_id;
                ELSE
                    RAISE WARNING 'Row continuity check failed: Flight ID=%, Last Arrival=%, Current Departure=%', current_row.id, last_row.arrival_airport_id, current_row.departure_airport_id;
                END IF;
            END IF;
            -- Update last_row to hold the current_row for the next iteration
            last_row := current_row;
        END LOOP;
END;
$$;

-- scheduled_flight insert procedure
-- make sure that the departure airport of the flight being inserted is the same as the arrival airport of the last flight
CREATE OR REPLACE PROCEDURE insert_scheduled_flight (new_flight RECORD)
LANGUAGE plpgsql
AS $$
DECLARE
    last_flight RECORD;
    -- To hold the most recent flight for the plane
BEGIN
    -- Fetch the most recent flight for the given plane
    SELECT
        * INTO last_flight
    FROM
        airline_booking2.scheduled_flight
    WHERE
        plane_id = new_flight.plane_id
    ORDER BY
        departure_time DESC
    LIMIT 1;
    -- Perform the continuity check if a previous flight exists
    IF last_flight IS NOT NULL THEN
        IF last_flight.arrival_airport_id != new_flight.departure_airport_id THEN
            RAISE EXCEPTION 'Continuity check failed: Last arrival airport (ID=%) does not match current departure airport (ID=%)', last_flight.arrival_airport_id, new_flight.departure_airport_id;
        END IF;
    END IF;
    -- Insert the new scheduled flight into the database
    INSERT INTO airline_booking2.scheduled_flight (departure_time, arrival_time, plane_id, departure_airport_id, arrival_airport_id, overbooking_id)
        VALUES (new_flight.departure_time, new_flight.arrival_time, new_flight.plane_id, new_flight.departure_airport_id, new_flight.arrival_airport_id, new_flight.overbooking_id);
END;
$$;

CREATE TYPE scheduled_flight_type AS (
    departure_time timestamp,
    arrival_time timestamp,
    plane_id int,
    departure_airport_id int,
    arrival_airport_id int,
    overbooking_id int
);

---------------------------------------------------------------
-- Sample Data
---------------------------------------------------------------

INSERT INTO airline_booking2.plane_type (plane_name)
    VALUES ('Boeing 737-200'),
    ('Boeing 737-220'),
    ('Boeing 747-400'),
    ('Boeing 757-020'),
    ('Airbus A300-03');

INSERT INTO airline_booking2.seat_type (seat_type)
    VALUES ('coach'),
    ('business class'),
    ('first class');

INSERT INTO airline_booking2.overbooking_rate (rate)
    VALUES (2.25);

INSERT INTO airline_booking2.airport (code, address)
    VALUES ('JFK', 'Queens, NY 11430, US'),
    ('LAX', '1 World Way, Los Angeles, CA 90045, US'),
    ('SEA', '17801 International Blvd, Pmb 68727, Seattle WA 98158, US'),
    ('ORD', '10000 W Balmoral Ave, Chicago IL 60666, US'),
    ('MDW', '5700 S Cicero Ave, Chicago IL 60638, US'),
    ('SFO', 'San Francisco, CA 94128, US'),
    ('SLC', 'W Terminal Dr, Salt Lake City, UT 84122, US'),
    ('DEN', '8500 Pena Blvd. Denver, CO 80249, US'),
    ('AGS', '1501 Aviation Way, Augusta, GA 30906, US'),
    ('ANC', '5000 W International Airport Rd, Anchorage, AK 99502, US');

INSERT INTO airline_booking2.plane_type_seat_type (plane_type_id, seat_type_id, quantity)
    VALUES (1, 1, 120),
    (2, 1, 165),
    (2, 2, 14),
    (3, 1, 220),
    (3, 2, 21),
    (3, 3, 6),
    (4, 1, 185),
    (4, 2, 18),
    (4, 3, 8),
    (5, 1, 170),
    (5, 2, 12),
    (5, 3, 6);

INSERT INTO airline_booking2.plane (plane_type_id)
    VALUES (1), -- 5 Boeing 737-200 planes
    (1),
    (1),
    (1),
    (1),
    (2), -- 5 Boeing 737-220 planes
    (2),
    (2),
    (2),
    (2),
    (3), -- 5 Boeing 747-400 planes
    (3),
    (3),
    (3),
    (3),
    (4), -- 3 Boeing 757-020 planes
    (4),
    (4),
    (5), -- 2 Airbus A300-03 planes
    (5);

INSERT INTO airline_booking2.scheduled_flight (departure_time, arrival_time, plane_id, departure_airport_id, arrival_airport_id, overbooking_id)
    VALUES ('2024-08-16 04:30:00', '2024-08-16 06:35:00', 1, 7, 2, 1),
    ('2024-08-16 06:50:00', '2024-08-16 13:00:00', 1, 2, 1, 1),
    ('2024-08-16 13:15:00', '2024-08-16 19:30:00', 1, 1, 3, 1),
    ('2024-08-21 07:00:00', '2024-08-21 09:00:00', 1, 2, 3, 1),
    ('2024-08-22 04:30:00', '2024-08-22 06:30:00', 2, 3, 5, 1),
    ('2024-08-16 07:30:00', '2024-08-16 09:30:00', 2, 10, 9, 1),
    ('2024-08-16 09:45:00', '2024-08-16 12:00:00', 2, 9, 5, 1),
    ('2024-08-16 12:15:00', '2024-08-16 14:15:00', 2, 5, 6, 1),
    ('2024-08-21 09:45:00', '2024-08-21 11:45:00', 1, 3, 2, 1);

INSERT INTO airline_booking2.flight_history (scheduled_flight_id, plane_id, actual_departure_time, actual_arrival_time)
    VALUES (1, 1, '2024-08-16 04:40:00', '2024-08-16 06:45:00'),
    (2, 1, '2024-08-16 07:20:00', '2024-08-16 13:20:00'),
    (4, 2, '2024-08-16 07:40:00', '2024-08-16 09:40:00'),
    (5, 2, '2024-08-16 10:05:00', '2024-08-16 12:20:00'),
    (6, 2, NULL, NULL);

INSERT INTO airline_booking2.passenger (passenger_name, passport_id, phone, email, address)
    VALUES ('Thomas Jones', '123456789', '801-420-6666', 'thomas@gmail.com', '123 W 456 S, Seattle, WA'),
    ('Sarah Martin', '987654321', '123-456-7890', 'sarah@gmail.com', '321 W 654 S, Salt Lake City, UT'),
    ('Chris Young', '123789456', '666-420-6969', 'chris@gmail.com', '222 W 222 S, Denver, CO'),
    ('Taft Allen', '111222333', '420-666-6969', 'taft@gmail.com', '111 W 111 S, Los Angeles, CA'),
    ('Ricardo Ruiz', '333222111', '420-111-2222', 'ricardo@gmail.com', '420 Ave 666, San Francisco, CA'),
    ('Cody Howell', '444555666', '801-420-7777', 'cody@gmail.com', '121 W 323 S, Chicago, IL'),
    ('Nathan Howell', '777888999', '801-420-8888', 'nathan@gmail.com', '333 W 444 S, Layton, UT');

INSERT INTO airline_booking2.reservation (passenger_id, scheduled_flight_id, ticket_cost, seat_type_id, seat_count)
    VALUES (1, 1, 200.00, 1, 1),
    (2, 1, 200.00, 1, 1),
    (3, 2, 150.00, 1, 1),
    (6, 5, 200.00, 1, 1),
    (5, 5, 150.00, 1, 1),
    (7, 6, 200.00, 1, 1),
    (1, 7, 200.00, 1, 1),
    (2, 7, 200.00, 1, 1),
    (5, 8, 150.00, 1, 1),
    (7, 9, 200.00, 1, 1),
    (6, 9, 200.00, 1, 1);

INSERT INTO airline_booking2.seat (reservation_id, printed_boarding_pass_at, seat_number, passenger_id)
    VALUES (1, '2024-08-16 02:30:00', 1, 1),
    (1, '2024-08-16 02:31:00', 2, 4),
    (2, '2024-08-16 04:53:00', 3, 2),
    (3, '2024-08-16 05:31:00', 20, 3),
    (4, '2024-08-16 02:00:00', 30, 6),
    (5, DEFAULT, DEFAULT, DEFAULT),
    (6, '2024-08-16 04:30:00', 31, 7);

INSERT INTO airline_booking2.payment (reservation_id, amount)
    VALUES (1, 400.00),
    (1, 3.99),
    (1, 5.78),
    (2, 200.00),
    (2, 1.99),
    (3, 150.00),
    (4, 200.00),
    (5, 150.00),
    (6, 200.00),
    (6, -200.00),
    (7, 200.00),
    (8, 200.00),
    (9, 150.00),
    (10, 200.00),
    (11, 200.00);

INSERT INTO airline_booking2.product (concession_name, price)
    VALUES ('Pillow', 8.16),
    ('Blanket', 6.12),
    ('Headphones', 20.99),
    ('Candy bar', 3.99),
    ('Fountain drink', 2.89),
    ('Chewing gum', 1.99);

---------------------------------------------------------------
-- Views
---------------------------------------------------------------

-- a view to get the total flight time for every plane
CREATE VIEW plane_total_flight_time AS
SELECT
    p.id plane_id,
    sum(age(fh.actual_arrival_time, fh.actual_departure_time)) total_hours_flight_time
FROM
    airline_booking2.flight_history fh
    INNER JOIN airline_booking2.plane p ON (p.id = fh.plane_id)
WHERE (fh.actual_departure_time IS NOT NULL
    AND fh.actual_arrival_time IS NOT NULL)
GROUP BY
    p.id
ORDER BY
    p.id;

-- a view to get the expenses for each passenger on a flight
-- (passengers have multiple entries if they took multiple flights)
CREATE VIEW customer_flight_expenses AS
SELECT
    p.id passenger_id,
    p.passenger_name,
    sf.departure_time::date flight_departure_date,
    sf.plane_id,
    ad.code departure_airport_code,
    aa.code arrival_airport_code,
    sum(pay.amount) amount_spent
FROM
    airline_booking2.payment pay
    INNER JOIN airline_booking2.reservation r ON (pay.reservation_id = r.id)
    INNER JOIN airline_booking2.scheduled_flight sf ON (sf.id = r.scheduled_flight_id)
    INNER JOIN airline_booking2.passenger p ON (p.id = r.passenger_id)
    INNER JOIN airline_booking2.airport ad ON (ad.id = sf.departure_airport_id)
    INNER JOIN airline_booking2.airport aa ON (aa.id = sf.arrival_airport_id)
GROUP BY
    p.id,
    p.passenger_name,
    sf.plane_id,
    flight_departure_date,
    departure_airport_code,
    arrival_airport_code
ORDER BY
    passenger_name ASC;

