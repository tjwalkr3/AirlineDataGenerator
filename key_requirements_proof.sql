-- gets the passenger count and plane capacity for each flight
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
	select sf.id as scheduled_flight_id, sf.plane_id, count(*) as reservation_count
	from scheduled_flight sf 
	inner join reservation r
	on (sf.id = r.scheduled_flight_id)
	group by sf.id, sf.plane_id
)
select pcts.scheduled_flight_id, 
	pc.plane_capacity, 
	pcts.reservation_count, 
	(pcts.reservation_count / pc.plane_capacity::decimal * 100) as percent_booked
from plane_capacities pc
inner join passenger_counts pcts
on (pc.plane_id = pcts.plane_id)
order by pcts.scheduled_flight_id asc;

-- flight performance efficiency function
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

-- Flight Estimations Query/Function
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