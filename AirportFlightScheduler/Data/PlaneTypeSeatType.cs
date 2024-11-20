using System;
using System.Collections.Generic;

namespace AirportFlightScheduler.Data;

public partial class PlaneTypeSeatType
{
    public int Id { get; set; }

    public int PlaneTypeId { get; set; }

    public int SeatTypeId { get; set; }

    public int Quantity { get; set; }

    public virtual PlaneType PlaneType { get; set; } = null!;

    public virtual SeatType SeatType { get; set; } = null!;
}
