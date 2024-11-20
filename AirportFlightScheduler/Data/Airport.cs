using System;
using System.Collections.Generic;

namespace AirportFlightScheduler.Data;

public partial class Airport
{
    public int Id { get; set; }

    public string Code { get; set; } = null!;

    public string Address { get; set; } = null!;

    public virtual ICollection<ScheduledFlight> ScheduledFlightArrivalAirports { get; set; } = new List<ScheduledFlight>();

    public virtual ICollection<ScheduledFlight> ScheduledFlightDepartureAirports { get; set; } = new List<ScheduledFlight>();
}
