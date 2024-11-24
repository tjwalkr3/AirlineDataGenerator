using System;
using System.Collections.Generic;

namespace AirportFlightScheduler.Data;

public partial class Passenger
{
    public int Id { get; set; }

    public string PassengerName { get; set; } = null!;

    public string? PassportId { get; set; }

    public string Phone { get; set; } = null!;

    public string? Email { get; set; }

    public string Address { get; set; } = null!;

    public virtual ICollection<Reservation> Reservations { get; set; } = new List<Reservation>();
}
