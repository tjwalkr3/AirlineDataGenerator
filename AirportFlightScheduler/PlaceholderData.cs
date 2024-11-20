namespace AirportFlightScheduler;

public static class PlaceholderData
{
    private static Random rand = new Random();

    public static string GeneratePassengerName() =>
        $"passenger-{rand.Next(100000000, 1000000000)}";

    public static string GeneratePassportId() =>
        $"{rand.Next(100000000, 1000000000)}";

    public static string GeneratePhoneNumber() =>
        $"({rand.Next(100, 1000)}) {rand.Next(100, 1000)}-{rand.Next(1000, 10000)}";

    public static string GenerateEmail() =>
        $"{Guid.NewGuid().ToString("N").Substring(0, 10)}@{Guid.NewGuid().ToString("N").Substring(0, 9)}.com";

    public static string GenerateAddress()
    {
        string[] streets = { "Main St", "Maple Ave", "Elm Dr", "Oak Blvd", "Pine Ct" };
        string[] cities = { "Springfield", "Riverton", "Hillcrest", "Lakeside", "Fairview" };
        string[] states = { "NY", "CA", "TX", "FL", "WA" };

        return $"{rand.Next(1, 9999)} {streets[rand.Next(streets.Length)]}, " +
               $"{cities[rand.Next(cities.Length)]}, {states[rand.Next(states.Length)]} " +
               $"{rand.Next(10000, 99999)}";
    }

    // Dictionary to map (Airport1, Airport2) -> Flight Time in Minutes
    public static Dictionary<(int, int), int> flightTimes = new()
    {
        // JFK flights
        { (1, 2), 360 },  // JFK to LAX
        { (2, 1), 360 },  // LAX to JFK
        { (1, 3), 330 },  // JFK to SEA
        { (3, 1), 330 },  // SEA to JFK
        { (1, 4), 120 },  // JFK to ORD
        { (4, 1), 120 },  // ORD to JFK
        { (1, 5), 125 },  // JFK to MDW
        { (5, 1), 125 },  // MDW to JFK
        { (1, 6), 350 },  // JFK to SFO
        { (6, 1), 350 },  // SFO to JFK
        { (1, 7), 300 },  // JFK to SLC
        { (7, 1), 300 },  // SLC to JFK
        { (1, 8), 240 },  // JFK to DEN
        { (8, 1), 240 },  // DEN to JFK
        { (1, 9), 140 },  // JFK to AGS
        { (9, 1), 140 },  // AGS to JFK
        { (1, 10), 420 }, // JFK to ANC
        { (10, 1), 420 }, // ANC to JFK

        // LAX flights
        { (2, 3), 150 },  // LAX to SEA
        { (3, 2), 150 },  // SEA to LAX
        { (2, 4), 240 },  // LAX to ORD
        { (4, 2), 240 },  // ORD to LAX
        { (2, 5), 250 },  // LAX to MDW
        { (5, 2), 250 },  // MDW to LAX
        { (2, 6), 90 },   // LAX to SFO
        { (6, 2), 90 },   // SFO to LAX
        { (2, 7), 120 },  // LAX to SLC
        { (7, 2), 120 },  // SLC to LAX
        { (2, 8), 120 },  // LAX to DEN
        { (8, 2), 120 },  // DEN to LAX
        { (2, 9), 300 },  // LAX to AGS
        { (9, 2), 300 },  // AGS to LAX
        { (2, 10), 300 }, // LAX to ANC
        { (10, 2), 300 }, // ANC to LAX

        // SEA flights
        { (3, 4), 240 },  // SEA to ORD
        { (4, 3), 240 },  // ORD to SEA
        { (3, 5), 245 },  // SEA to MDW
        { (5, 3), 245 },  // MDW to SEA
        { (3, 6), 120 },  // SEA to SFO
        { (6, 3), 120 },  // SFO to SEA
        { (3, 7), 120 },  // SEA to SLC
        { (7, 3), 120 },  // SLC to SEA
        { (3, 8), 150 },  // SEA to DEN
        { (8, 3), 150 },  // DEN to SEA
        { (3, 9), 330 },  // SEA to AGS
        { (9, 3), 330 },  // AGS to SEA
        { (3, 10), 180 }, // SEA to ANC
        { (10, 3), 180 }, // ANC to SEA

        // ORD flights
        { (4, 5), 20 },   // ORD to MDW
        { (5, 4), 20 },   // MDW to ORD
        { (4, 6), 240 },  // ORD to SFO
        { (6, 4), 240 },  // SFO to ORD
        { (4, 7), 180 },  // ORD to SLC
        { (7, 4), 180 },  // SLC to ORD
        { (4, 8), 120 },  // ORD to DEN
        { (8, 4), 120 },  // DEN to ORD
        { (4, 9), 120 },  // ORD to AGS
        { (9, 4), 120 },  // AGS to ORD
        { (4, 10), 360 }, // ORD to ANC
        { (10, 4), 360 }, // ANC to ORD

        // MDW flights
        { (5, 6), 245 },  // MDW to SFO
        { (6, 5), 245 },  // SFO to MDW
        { (5, 7), 180 },  // MDW to SLC
        { (7, 5), 180 },  // SLC to MDW
        { (5, 8), 120 },  // MDW to DEN
        { (8, 5), 120 },  // DEN to MDW
        { (5, 9), 125 },  // MDW to AGS
        { (9, 5), 125 },  // AGS to MDW
        { (5, 10), 365 }, // MDW to ANC
        { (10, 5), 365 }, // ANC to MDW

        // SFO flights
        { (6, 7), 120 },  // SFO to SLC
        { (7, 6), 120 },  // SLC to SFO
        { (6, 8), 120 },  // SFO to DEN
        { (8, 6), 120 },  // DEN to SFO
        { (6, 9), 300 },  // SFO to AGS
        { (9, 6), 300 },  // AGS to SFO
        { (6, 10), 330 }, // SFO to ANC
        { (10, 6), 330 }, // ANC to SFO

        // SLC flights
        { (7, 8), 90 },   // SLC to DEN
        { (8, 7), 90 },   // DEN to SLC
        { (7, 9), 240 },  // SLC to AGS
        { (9, 7), 240 },  // AGS to SLC
        { (7, 10), 300 }, // SLC to ANC
        { (10, 7), 300 }, // ANC to SLC

        // DEN flights
        { (8, 9), 180 },  // DEN to AGS
        { (9, 8), 180 },  // AGS to DEN
        { (8, 10), 300 }, // DEN to ANC
        { (10, 8), 300 }, // ANC to DEN

        // AGS flights
        { (9, 10), 540 }, // AGS to ANC
        { (10, 9), 540 }  // ANC to AGS
    };
}
