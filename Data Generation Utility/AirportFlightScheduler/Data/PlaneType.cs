using System;
using System.Collections.Generic;

namespace AirportFlightScheduler.Data;

public partial class PlaneType
{
    public int Id { get; set; }

    public string PlaneName { get; set; } = null!;

    public virtual ICollection<PlaneTypeSeatType> PlaneTypeSeatTypes { get; set; } = new List<PlaneTypeSeatType>();

    public virtual ICollection<Plane> Planes { get; set; } = new List<Plane>();
}
