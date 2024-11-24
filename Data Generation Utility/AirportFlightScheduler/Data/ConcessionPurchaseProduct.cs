using System;
using System.Collections.Generic;

namespace AirportFlightScheduler.Data;

public partial class ConcessionPurchaseProduct
{
    public int Id { get; set; }

    public int ProductId { get; set; }

    public int ConcessionPurchaseId { get; set; }

    public int Quantity { get; set; }

    public virtual ConcessionPurchase ConcessionPurchase { get; set; } = null!;

    public virtual Product Product { get; set; } = null!;
}
