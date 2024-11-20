using System;
using System.Collections.Generic;

namespace AirportFlightScheduler.Data;

public partial class Payment
{
    public int Id { get; set; }

    public int ReservationId { get; set; }

    public decimal Amount { get; set; }

    public virtual ICollection<ConcessionPurchase> ConcessionPurchases { get; set; } = new List<ConcessionPurchase>();

    public virtual Reservation Reservation { get; set; } = null!;
}
