using System;
using System.Collections.Generic;

namespace AirportFlightScheduler.Data;

public partial class Product
{
    public int Id { get; set; }

    public string ConcessionName { get; set; } = null!;

    public decimal Price { get; set; }

    public virtual ICollection<ConcessionPurchaseProduct> ConcessionPurchaseProducts { get; set; } = new List<ConcessionPurchaseProduct>();
}
