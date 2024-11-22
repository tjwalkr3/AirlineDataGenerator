// Main driver for console creation of queries to insert into scheduled_flight
using Microsoft.Extensions.Configuration;
using Microsoft.EntityFrameworkCore;
using AirportFlightScheduler.Data;

namespace AirportFlightScheduler;

public class Program
{
    static async Task Main(string[] args)
    {
        DbContextOptions<AirlineContext> options;

        //Initialize DbContextOptions object w/ user secret
        try
        {
            var configuration = new ConfigurationBuilder().AddUserSecrets<Program>().Build();
            var connectionString = configuration.GetConnectionString("DefaultConnection");
            options = new DbContextOptionsBuilder<AirlineContext>().UseNpgsql(connectionString).Options;
            Console.WriteLine($"DBContext successfully created new options with connection string: \n{connectionString}\n");
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex.ToString());
            return;
        }

        Console.WriteLine("Press any key to start data generation...");
        Console.ReadLine();

        Console.WriteLine("Generating data...");
        FlightDataGenerator generator = new(options);
        await generator.GenerateData(15, 3);
        Console.WriteLine("Done.");
    }
}