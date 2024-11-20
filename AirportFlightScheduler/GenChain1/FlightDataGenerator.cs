using AirportFlightScheduler.Data;
using Microsoft.EntityFrameworkCore;
using static System.Runtime.InteropServices.JavaScript.JSType;

namespace AirportFlightScheduler.GenChain1;

public class FlightDataGenerator
{
    private AirlineContext _context;

    public FlightDataGenerator(AirlineContext context)
    {
        _context = context;
    }

    // Iterate through the data generations functions a set number of times
    public async Task GenerateData(int iterations, int numProducts)
    {
        for (int i = 0; i < iterations; i++)
        {
            Console.Write("\nCompleted Phase: ");
            await GenerateSingleDay(numProducts);
            Console.WriteLine($"Done with day: {i + 1}/{iterations}");
        }
    }

    // Run all of the data generation functions
    private async Task GenerateSingleDay(int numProducts)
    {
        // Data Generation Chain 1
        
        List<int> scheduledFlightIds = await GenerateScheduledFlights();
        Console.Write("1");
        List<int> flightHistoryIds = await GenerateFlightHistory(scheduledFlightIds);
        Console.Write("2");
        List<int> reservationIds = await GenerateReservations(scheduledFlightIds);
        Console.Write("3");
        List<int> seatIds = await GenerateSeats(reservationIds);
        Console.Write("4");
        List<int> paymentIds = await GeneratePayments(reservationIds);
        Console.Write("5");

        // Data Generation Chain 2 (A LOT SIMPLER)
        await GenerateConcessionPurchases(seatIds, numProducts);
        Console.Write("6\n");
    }

    // Generate a given number of fake passengers
    private async Task<List<int>> GeneratePassengers(int passengerCount)
    {
        List<Passenger> passengers = [];

        // Create all the fake passengers using info from the DataGenerator class
        for (int i = 0; i < passengerCount; i++)
        {
            passengers.Add(new Passenger
            {
                PassengerName = PlaceholderData.GeneratePassengerName(),
                PassportId = PlaceholderData.GeneratePassportId(),
                Phone = PlaceholderData.GeneratePhoneNumber(),
                Email = PlaceholderData.GenerateEmail(),
                Address = PlaceholderData.GenerateAddress()
            });
        }

        // Add entities to the context, save them, and return the IDs
        await _context.Passengers.AddRangeAsync(passengers);
        await _context.SaveChangesAsync();
        return passengers.Select(entity => entity.Id).ToList();
    }

    // Generate one day worth of flights in the in the ScheduledFlight table
    private async Task<List<int>> GenerateScheduledFlights()
    {
        // Get the latest flight from the ScheduledFlight table
        var latestScheduledFlight = await _context.ScheduledFlights
            .OrderByDescending(f => f.ArrivalTime)
            .FirstOrDefaultAsync();

        // Set the DateTime to 1/1/2017 if no flights are present in the database
        DateTime startOfNextDay = latestScheduledFlight == null
            ? new DateTime(2017, 1, 1)
            : latestScheduledFlight.ArrivalTime.Date.AddDays(1);

        Random rand = new Random();

        // Get list of available planes
        List<Plane> planes = _context.Planes.ToList();

        // Calculate the number of minutes of air time per plane (90% of the 1440 minutes in a day)
        double timePerPlane = 1440 * 0.9;

        // Schedule flights
        List<ScheduledFlight> newFlights = [];
        foreach (var plane in planes)
        {
            var lastFlight = _context.ScheduledFlights
                .Where(f => f.PlaneId == plane.Id)
                .OrderByDescending(f => f.ArrivalTime)
                .FirstOrDefault();

            // Set the earliest flight to start at the beginning of the day, and get the airport it's at currently at (if none, default to Chicago ORD). 
            int planeCurrentLocation = lastFlight?.ArrivalAirportId ?? 4;
            DateTime planeLastArrival = startOfNextDay;
            TimeSpan remainingTime = TimeSpan.FromMinutes(timePerPlane);

            while (remainingTime.TotalMinutes > 0)
            {
                // Filter the flight times for flights that can fit in the remaining time in the day
                var validAirportPairs = PlaceholderData.flightTimes
                    .Where(f => f.Key.Item1 == planeCurrentLocation && f.Value <= remainingTime.TotalMinutes)
                    .Select(f => f.Key)
                    .ToList();

                // Ensure there are valid airport pairs to choose from
                if (validAirportPairs.Count == 0)
                {
                    break;
                }

                var randomAirportPair = validAirportPairs.ElementAt(rand.Next(validAirportPairs.Count));
                int flightDuration = PlaceholderData.flightTimes[randomAirportPair];

                remainingTime = remainingTime.Subtract(TimeSpan.FromMinutes(flightDuration));
                DateTime departureTime = planeLastArrival;
                DateTime arrivalTime = departureTime.AddMinutes(flightDuration);

                // Create a new scheduled flight
                newFlights.Add(new ScheduledFlight
                {
                    DepartureTime = departureTime,
                    ArrivalTime = arrivalTime,
                    PlaneId = plane.Id,
                    DepartureAirportId = randomAirportPair.Item1,
                    ArrivalAirportId = randomAirportPair.Item2,
                    OverbookingId = _context.OverbookingRates.Single().Id
                });

                // Update the plane's current location to the arrival airport
                planeCurrentLocation = randomAirportPair.Item2;
                planeLastArrival = departureTime.AddMinutes(flightDuration);
            }
        }

        // Add the new flights to the context and save them
        await _context.ScheduledFlights.AddRangeAsync(newFlights);
        await _context.SaveChangesAsync();

        // Return a list of the scheduled flight IDs
        return newFlights.Select(f => f.Id).ToList();
    }

    // Generate one entry in the flight history for every entry in the scheduled flights
    private async Task<List<int>> GenerateFlightHistory(List<int> scheduledFlightIds)
    {
        var scheduledFlights = _context.ScheduledFlights
            .Where(s => scheduledFlightIds.Contains(s.Id))
            .ToList();

        List<FlightHistory> flightHistoryEntries = scheduledFlights.Select(s => new FlightHistory
        {
            ScheduledFlightId = s.Id,
            PlaneId = s.PlaneId,
            ActualDepartureTime = s.DepartureTime,
            ActualArrivalTime = s.ArrivalTime
        }).ToList();

        _context.FlightHistories.AddRange(flightHistoryEntries);
        await _context.SaveChangesAsync();

        // Return the ids of the created payments
        return flightHistoryEntries.Select(h => h.Id).ToList();
    }

    // Given a list of scheduledFlightIds, generate all the necessary reservations in the reservation table
    public async Task<List<int>> GenerateReservations(List<int> scheduledFlightIds)
    {
        var reservations = new List<Reservation>();

        foreach (var scheduledFlightId in scheduledFlightIds)
        {
            var scheduledFlight = await _context.ScheduledFlights.FindAsync(scheduledFlightId);
            if (scheduledFlight == null) continue;

            var plane = await _context.Planes.FindAsync(scheduledFlight.PlaneId);
            if (plane == null) continue;

            // Initialize seat availability for this plane
            var allSeatTypes = await _context.SeatTypes.ToListAsync();
            var planeTypeSeatTypes = await _context.PlaneTypeSeatTypes
                .Where(ptst => ptst.PlaneTypeId == plane.PlaneTypeId)
                .ToListAsync();

            var seatAvailability = allSeatTypes.ToDictionary(
                st => st.Id,
                st => planeTypeSeatTypes.FirstOrDefault(ptst => ptst.SeatTypeId == st.Id)?.Quantity ?? 0
            );

            var totalSeats = seatAvailability.Values.Sum();
            var remainingReservations = (int)(totalSeats * 1.1);

            // Generate passengers for this flight
            var passengers = await GeneratePassengers(remainingReservations);
            var passengerQueue = new Queue<int>(passengers);

            var (departureAirport, arrivalAirport) = (scheduledFlight.DepartureAirportId, scheduledFlight.ArrivalAirportId);
            var flightTimeMinutes = PlaceholderData.flightTimes[(departureAirport, arrivalAirport)];

            // Fill seats, prioritizing first class, business, then coach
            foreach (var seatTypeId in seatAvailability.Keys.OrderByDescending(k => k)) // Higher seat types prioritized
            {
                int maxReservations;

                if (seatTypeId == 1)
                {
                    maxReservations = remainingReservations; // fill all the rest of the booking limit with coach seats
                }
                else
                {
                    maxReservations = seatAvailability[seatTypeId]; // Only fill the first class and business class sections to their limit
                }

                for (int i = 0; i < maxReservations; i++)
                {
                    if (!passengerQueue.TryDequeue(out var passengerId))
                    {
                        throw new InvalidOperationException("Not enough passengers generated for reservations.");
                    }

                    var ticketCost = seatTypeId switch
                    {
                        1 => flightTimeMinutes * 0.3,       // Coach
                        2 => flightTimeMinutes * 0.3 * 2,   // Business
                        3 => flightTimeMinutes * 0.3 * 3,   // First Class
                        _ => throw new InvalidOperationException("Unknown seat type")
                    };

                    reservations.Add(new Reservation
                    {
                        PassengerId = passengerId,
                        ScheduledFlightId = scheduledFlight.Id,
                        TicketCost = (decimal)ticketCost,
                        SeatTypeId = seatTypeId,
                        SeatCount = 1
                    });
                }

                // Deduct reserved seats and remaining reservations
                seatAvailability[seatTypeId] -= maxReservations;
                remainingReservations -= maxReservations;
            }
        }

        // Save the reservations to the database
        _context.Reservations.AddRange(reservations);
        await _context.SaveChangesAsync();

        // Return the IDs of the created reservations
        return reservations.Select(f => f.Id).ToList();
    }

    // Given a list of reservationIDs, generate all the necessary seats in the seat table
    public async Task<List<int>> GenerateSeats(List<int> reservationIds)
    {
        var seats = new List<Seat>();
        var reservations = _context.Reservations
            .Where(r => reservationIds.Contains(r.Id))
            .ToList();

        if (reservations.Count != reservationIds.Count)
        {
            throw new ArgumentException("Some reservation IDs are invalid.");
        }

        // Group reservations by ScheduledFlightId
        var reservationsByFlight = reservations.GroupBy(r => r.ScheduledFlightId);

        foreach (var flightGroup in reservationsByFlight)
        {
            var scheduledFlightId = flightGroup.Key;
            var scheduledFlight = await _context.ScheduledFlights.FindAsync(scheduledFlightId);
            if (scheduledFlight == null) continue;

            // Initialize seat availability for this flight by seat type
            var totalSeatsByType = _context.PlaneTypeSeatTypes
                .Where(ptst => ptst.PlaneTypeId == scheduledFlight.Plane.PlaneTypeId)
                .ToDictionary(
                    ptst => ptst.SeatTypeId,
                    ptst => ptst.Quantity
                );

            int assignedSeatsCount = 0;

            // Process reservations for this flight
            foreach (var reservation in flightGroup)
            {
                var passengerSeats = reservation.SeatCount;

                for (int i = 0; i < passengerSeats; i++)
                {
                    // Determine if the passenger can be assigned a seat or is overbooked
                    bool canAssignSeat = totalSeatsByType[reservation.SeatTypeId] > 0;

                    seats.Add(new Seat
                    {
                        ReservationId = reservation.Id,
                        PrintedBoardingPassAt = canAssignSeat ? scheduledFlight.DepartureTime.AddHours(-1) : null,
                        SeatNumber = canAssignSeat ? ++assignedSeatsCount : null,
                        PassengerId = reservation.PassengerId
                    });

                    // Decrease the remaining seat count for this seat type if a seat was assigned
                    if (canAssignSeat)
                    {
                        totalSeatsByType[reservation.SeatTypeId]--;
                    }
                }
            }
        }

        await _context.Seats.AddRangeAsync(seats);
        await _context.SaveChangesAsync();

        // Return the IDs of the created seats
        return seats.Select(s => s.Id).ToList();
    }

    // Given a list of reservationIDs, generate all the necessary payments in the payment table
    public async Task<List<int>> GeneratePayments(List<int> reservationIds)
    {
        var payments = new List<Payment>();

        // Load reservations in bulk
        var reservations = await _context.Reservations
            .Where(r => reservationIds.Contains(r.Id))
            .ToListAsync();

        if (reservations.Count != reservationIds.Count)
        {
            throw new ArgumentException("Some reservation IDs are invalid.");
        }

        // Load seats in bulk and group by ReservationId
        var seatCounts = await _context.Seats
            .Where(s => reservationIds.Contains(s.ReservationId) && s.PrintedBoardingPassAt != null)
            .GroupBy(s => s.ReservationId)
            .ToDictionaryAsync(g => g.Key, g => g.Count());

        // Generate payments
        foreach (var reservation in reservations)
        {
            var seatCountWithPass = seatCounts.TryGetValue(reservation.Id, out var count) ? count : 0;
            if (seatCountWithPass == 0) continue;
            var amount = reservation.TicketCost * seatCountWithPass;

            payments.Add(new Payment
            {
                ReservationId = reservation.Id,
                Amount = amount
            });
        }

        // Save the payments to the database
        await _context.Payments.AddRangeAsync(payments);
        await _context.SaveChangesAsync();

        // Return the IDs of the created payments
        return payments.Select(f => f.Id).ToList();
    }

    // Given a list of seats, create entries in the payment, concession_purchase, and concession_purchase_product table (for a given products) 
    public async Task GenerateConcessionPurchases(List<int> seatIds, int numProducts)
    {
        // Validate that the seat IDs exist
        var validSeats = await _context.Seats
            .Where(s => seatIds.Contains(s.Id) && s.PrintedBoardingPassAt != null)
            .ToListAsync();

        // Retrieve all products in one database call
        var products = await _context.Products.ToListAsync();
        if (products.Count < numProducts)
        {
            throw new InvalidOperationException("Not enough products available to fulfill the request.");
        }

        Random random = new Random();
        List<Payment> payments = new List<Payment>();
        List<ConcessionPurchase> concessionPurchases = new List<ConcessionPurchase>();
        List<ConcessionPurchaseProduct> concessionPurchaseProducts = new List<ConcessionPurchaseProduct>();

        foreach (var seat in validSeats)
        {
            // Select `numProducts` random products
            var selectedProducts = products.OrderBy(_ => random.Next()).Take(numProducts).ToList();

            // Calculate the total cost of the products
            decimal totalCost = selectedProducts.Sum(p => p.Price);

            // Create a Payment entry
            var payment = new Payment
            {
                Amount = totalCost,
                ReservationId = seat.ReservationId
            };
            payments.Add(payment);

            // Create a ConcessionPurchase entry and link it to the payment
            var concessionPurchase = new ConcessionPurchase
            {
                SeatId = seat.Id,
                Payment = payment // Link the Payment object directly
            };
            concessionPurchases.Add(concessionPurchase);

            // Add the ConcessionPurchaseProduct entries and link to the ConcessionPurchase
            foreach (var product in selectedProducts)
            {
                concessionPurchaseProducts.Add(new ConcessionPurchaseProduct
                {
                    ConcessionPurchase = concessionPurchase, // Link the ConcessionPurchase object directly
                    ProductId = product.Id,
                    Quantity = 1
                });
            }
        }

        // Bulk add all entities
        await _context.Payments.AddRangeAsync(payments);
        await _context.ConcessionPurchases.AddRangeAsync(concessionPurchases);
        await _context.ConcessionPurchaseProducts.AddRangeAsync(concessionPurchaseProducts);

        // Save all changes in a single database call
        await _context.SaveChangesAsync();
    }
}
