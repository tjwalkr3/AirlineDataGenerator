using System;
using System.Collections.Generic;

namespace AirportFlightScheduler.Data;

public partial class ConcessionPurchase
{
    public int Id { get; set; }

    public int PaymentId { get; set; }

    public int SeatId { get; set; }

    public virtual ICollection<ConcessionPurchaseProduct> ConcessionPurchaseProducts { get; set; } = new List<ConcessionPurchaseProduct>();

    public virtual Payment Payment { get; set; } = null!;

    public virtual Seat Seat { get; set; } = null!;
}
