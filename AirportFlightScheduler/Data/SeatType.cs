using System;
using System.Collections.Generic;

namespace AirportFlightScheduler.Data;

public partial class SeatType
{
    public int Id { get; set; }

    public string SeatType1 { get; set; } = null!;

    public virtual ICollection<PlaneTypeSeatType> PlaneTypeSeatTypes { get; set; } = new List<PlaneTypeSeatType>();

    public virtual ICollection<Reservation> Reservations { get; set; } = new List<Reservation>();
}
