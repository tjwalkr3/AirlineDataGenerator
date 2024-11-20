using System;
using System.Collections.Generic;

namespace AirportFlightScheduler.Data;

public partial class Plane
{
    public int Id { get; set; }

    public int PlaneTypeId { get; set; }

    public virtual ICollection<FlightHistory> FlightHistories { get; set; } = new List<FlightHistory>();

    public virtual PlaneType PlaneType { get; set; } = null!;

    public virtual ICollection<ScheduledFlight> ScheduledFlights { get; set; } = new List<ScheduledFlight>();
}
