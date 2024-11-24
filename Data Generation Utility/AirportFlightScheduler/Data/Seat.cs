using System;
using System.Collections.Generic;

namespace AirportFlightScheduler.Data;

public partial class Seat
{
    public int Id { get; set; }

    public int ReservationId { get; set; }

    public DateTime? PrintedBoardingPassAt { get; set; }

    public int? SeatNumber { get; set; }

    public int? PassengerId { get; set; }

    public virtual ICollection<ConcessionPurchase> ConcessionPurchases { get; set; } = new List<ConcessionPurchase>();

    public virtual Reservation Reservation { get; set; } = null!;
}
