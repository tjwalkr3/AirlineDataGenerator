using System;
using System.Collections.Generic;

namespace AirportFlightScheduler.Data;

public partial class FlightHistory
{
    public int Id { get; set; }

    public int ScheduledFlightId { get; set; }

    public int PlaneId { get; set; }

    public DateTime? ActualDepartureTime { get; set; }

    public DateTime? ActualArrivalTime { get; set; }

    public virtual Plane Plane { get; set; } = null!;

    public virtual ScheduledFlight ScheduledFlight { get; set; } = null!;
}
