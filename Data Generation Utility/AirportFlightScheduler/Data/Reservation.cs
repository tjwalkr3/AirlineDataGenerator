using System;
using System.Collections.Generic;

namespace AirportFlightScheduler.Data;

public partial class Reservation
{
    public int Id { get; set; }

    public int PassengerId { get; set; }

    public int ScheduledFlightId { get; set; }

    public decimal TicketCost { get; set; }

    public int SeatTypeId { get; set; }

    public int SeatCount { get; set; }

    public virtual Passenger Passenger { get; set; } = null!;

    public virtual ICollection<Payment> Payments { get; set; } = new List<Payment>();

    public virtual ScheduledFlight ScheduledFlight { get; set; } = null!;

    public virtual SeatType SeatType { get; set; } = null!;

    public virtual ICollection<Seat> Seats { get; set; } = new List<Seat>();
}
