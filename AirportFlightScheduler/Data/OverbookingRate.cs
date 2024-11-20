using System;
using System.Collections.Generic;

namespace AirportFlightScheduler.Data;

public partial class OverbookingRate
{
    public int Id { get; set; }

    public decimal Rate { get; set; }

    public virtual ICollection<ScheduledFlight> ScheduledFlights { get; set; } = new List<ScheduledFlight>();
}
