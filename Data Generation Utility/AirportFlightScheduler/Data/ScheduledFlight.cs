using System;
using System.Collections.Generic;

namespace AirportFlightScheduler.Data;

public partial class ScheduledFlight
{
    public int Id { get; set; }

    public DateTime DepartureTime { get; set; }

    public DateTime ArrivalTime { get; set; }

    public int PlaneId { get; set; }

    public int DepartureAirportId { get; set; }

    public int ArrivalAirportId { get; set; }

    public int OverbookingId { get; set; }

    public virtual Airport ArrivalAirport { get; set; } = null!;

    public virtual Airport DepartureAirport { get; set; } = null!;

    public virtual ICollection<FlightHistory> FlightHistories { get; set; } = new List<FlightHistory>();

    public virtual OverbookingRate Overbooking { get; set; } = null!;

    public virtual Plane Plane { get; set; } = null!;

    public virtual ICollection<Reservation> Reservations { get; set; } = new List<Reservation>();
}
