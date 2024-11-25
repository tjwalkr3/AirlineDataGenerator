-- Key Feature #1
-- This query gets each flight, how many have booked it, the max capacity, and its scheduled arrival and departure timestamps
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

-- Key Feature #2
-- This query gets each flight, how many have booked it, the max capacity, and its scheduled arrival and departure timestamps.
with plane_capacities as (
	select p.id as plane_id, sum(ptst.quantity) as plane_capacity
	from plane p
	inner join plane_type pt 
	on (p.plane_type_id = pt.id)
	inner join plane_type_seat_type ptst 
	on (ptst.plane_type_id = pt.id)
	group by p.id
),
reservation_counts as (
	select sf.id as scheduled_flight_id, sf.plane_id, sf.departure_time, sf.arrival_time, count(*) as reservation_count
	from scheduled_flight sf 
	inner join reservation r
	on (sf.id = r.scheduled_flight_id)
	group by sf.id, sf.plane_id, sf.departure_time, sf.arrival_time
),
occupied as (
	select sf.id, count(*) as printed_boarding_pass
	from reservation r
	inner join seat s
	on (r.id = s.reservation_id)
	inner join scheduled_flight sf 
	on (r.scheduled_flight_id = sf.id)
	where s.printed_boarding_pass_at is not null
	group by sf.id
)
select pcts.scheduled_flight_id, 
	pc.plane_capacity, 
	o.printed_boarding_pass,
	pcts.reservation_count
from plane_capacities pc
inner join reservation_counts pcts
on (pc.plane_id = pcts.plane_id)
inner join occupied o
on (pcts.scheduled_flight_id = o.id)
order by pcts.scheduled_flight_id asc;

-- Key Feature #3
-- Gets the total percentage of all seats sold, and the total percentage of passengers that have been refunded
with total_seats as (
    select pt.plane_name,
           ptst.plane_type_id,
           sum(ptst.quantity) as num_seats 
    from airline_booking2.plane_type_seat_type ptst
    inner join airline_booking2.seat_type st
        on (ptst.seat_type_id = st.id)
    inner join airline_booking2.plane_type pt
        on (ptst.plane_type_id = pt.id)
    group by pt.plane_name, ptst.plane_type_id
), seats_booked as (
    select p.plane_type_id,
           sf.id as flight_id,
           count(*) as num_booked
    from airline_booking2.seat s
    inner join airline_booking2.reservation r
        on (s.reservation_id = r.id)
    inner join airline_booking2.scheduled_flight sf 
        on (r.scheduled_flight_id = sf.id)
    inner join airline_booking2.plane p 
        on (sf.plane_id = p.id)
    where s.printed_boarding_pass_at is not null
    group by p.plane_type_id, sf.id
), overbooked_paid as (
    select sf.id as flight_id,
           count(*) as overbooked_paid_out
    from airline_booking2.reservation r
    inner join airline_booking2.scheduled_flight sf 
        on (r.scheduled_flight_id = sf.id)
    inner join airline_booking2.payment pay
        on (r.id = pay.reservation_id and pay.amount < 0) -- Negative payment indicates compensation
    group by sf.id
)
select 
    coalesce(
        cast(sum(coalesce(sb.num_booked, 0)) as decimal(15,5)) 
        / cast(sum(ts.num_seats) as decimal(15,5)) * 100, 
        0
    ) as percent_seats_sold,
    coalesce(
        cast(sum(coalesce(op.overbooked_paid_out, 0)) as decimal(15,5)) 
        / cast(sum(coalesce(sb.num_booked, 0)) as decimal(15,5)) * 100, 
        0
    ) as percent_passengers_refunded
from airline_booking2.scheduled_flight sf
inner join airline_booking2.plane p
    on (sf.plane_id = p.id)
inner join total_seats ts
    on (ts.plane_type_id = p.plane_type_id)
left join seats_booked sb
    on (sf.id = sb.flight_id)
left join overbooked_paid op
    on (sf.id = op.flight_id);

-- Key Feature #4
-- Flight Performance Efficiency function
-- Calculates percentages based on the flights that have been canceled.
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
