-- Key Feature #1
-- Gets each flight, how many have booked it, the max capacity, and its scheduled arrival and departure timestamps
with plane_capacities as (
	select p.id as plane_id, sum(ptst.quantity) as plane_capacity
	from plane p
	inner join plane_type pt 
	on (p.plane_type_id = pt.id)
	inner join plane_type_seat_type ptst 
	on (ptst.plane_type_id = pt.id)
	group by p.id
),
passenger_counts as (
	select sf.id as scheduled_flight_id, sf.plane_id, sf.departure_time, sf.arrival_time, count(*) as reservation_count
	from scheduled_flight sf 
	inner join reservation r
	on (sf.id = r.scheduled_flight_id)
	group by sf.id, sf.plane_id, sf.departure_time, sf.arrival_time
)
select pcts.scheduled_flight_id, 
	pc.plane_capacity, 
	pcts.reservation_count, 
	(pcts.reservation_count / pc.plane_capacity::decimal * 100) as percent_booked,
	pcts.departure_time,
	pcts.arrival_time
from plane_capacities pc
inner join passenger_counts pcts
on (pc.plane_id = pcts.plane_id)
order by pcts.scheduled_flight_id asc;

-- Key Feature #4
-- Flight Performance Efficiency function
-- calculates percentages based on the flights that have been canceled
create or replace function flight_performance_efficiency() returns table(percent_flights_on_time decimal(10,6), percent_flights_canceled decimal(10,6)) as $$
	begin
	return query with flight_counts as (
	select 
		count(*) as total_flights,
		sum(case 
				when fh.actual_departure_time is null and fh.actual_arrival_time is null then 1 
				else 0 
			end
		) as canceled_flights,
		sum(case 
				when fh.actual_departure_time is not null and fh.actual_arrival_time is not null then 1  
				else 0 
			end
		) as on_time_flights
	from airline_booking2.flight_history fh
	)
	select 
		(coalesce(on_time_flights, 0) * 100.0 / coalesce(total_flights, 1))::decimal(10,6) as percent_flights_on_time,
		(coalesce(canceled_flights, 0) * 100.0 / coalesce(total_flights, 1))::decimal(10,6) as percent_flights_canceled
	from flight_counts;
	end;
$$ language plpgsql;

select * from flight_performance_efficiency();

-- Key Feature 5
-- Flight Revenue Estimation Query/Function
-- Calculates the expected revenue within a 10 day interval after a given startdate
create or replace function flight_revenue_estimate(startdate date) 
returns table(start_date date, end_date date, revenue decimal(10,2)) as $$
begin
    return query 
    select
        (select min(departure_time)::date 
         from airline_booking2.scheduled_flight 
         where departure_time >= flight_revenue_estimate.startdate) as start_date,

        (startdate + interval '10 days')::date as end_date,

        sum(p.amount)::decimal(10,2) as revenue
    from airline_booking2.scheduled_flight sf
    inner join airline_booking2.reservation r
        on sf.id = r.scheduled_flight_id
    inner join airline_booking2.payment p
        on r.id = p.reservation_id
    where sf.departure_time >= flight_revenue_estimate.startdate
      and sf.arrival_time < (startdate + interval '10 days')
    group by start_date, end_date; -- Group by to return correct aggregates
end;
$$ language plpgsql;

select * from flight_estimate('08-21-24');

-- flight continuity procedure
-- tracks whether scheduled flights are from airport to airport
-- logs when a plane "teleports" to another airport without flying
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

CALL flight_continuity();
