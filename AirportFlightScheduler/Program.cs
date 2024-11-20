// Main driver for console creation of queries to insert into scheduled_flight
using Microsoft.Extensions.Configuration;
using Microsoft.EntityFrameworkCore;
using AirportFlightScheduler.Data;
using AirportFlightScheduler.GenChain1;

public class Program
{
    static async Task Main(string[] args)
    {
        AirlineContext context;

        //Initialize DBContext object
        try
        {
            var configuration = new ConfigurationBuilder().AddUserSecrets<Program>().Build();
            var connectionString = configuration.GetConnectionString("DefaultConnection");
            var options = new DbContextOptionsBuilder<AirlineContext>().UseNpgsql(connectionString).Options;
            context = new AirlineContext(options);
            Console.WriteLine($"DBContext successfully created with connection string: \n{connectionString}\n");
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex.ToString());
            return;
        }

        // Ask which entry path the user wants to 
        string DataGenChain = GetValidInput(new List<string> { "1" }, prompt: "Press 1 then [ENTER] to generate data!");

        Console.WriteLine("Generating data...");
        FlightDataGenerator generator = new(context);
        await generator.GenerateData(1, 3);
        Console.WriteLine("Done.");
    }

    /// < summary >
    /// This method allows passing a List of string values that classify as valid inputs, 
    /// a prompt to instruct users on what inputs to give, and a case sensitivity boolean 
    /// that determines whether or not user input must match the case of the valid inputs.
    /// </summary>
    static string GetValidInput(List<string> validInputs, string prompt, bool isCaseSensitive = false)
    {
        string invalidInputMsg = "Sorry, invalid input. Please try again.";
        Console.WriteLine(prompt);
        string? userInput = Console.ReadLine();

        if (isCaseSensitive)
        {
            while (userInput == null || !validInputs.Contains(userInput))
            {
                Console.WriteLine(invalidInputMsg);
                Console.WriteLine(prompt);
                userInput = Console.ReadLine();
            }
            return userInput;
        }

        // This code is only run for non-case-sensitive inputs
        List<string> lowercaseValidInputs = validInputs.ConvertAll(item => item.ToLower());
        while (userInput == null || !lowercaseValidInputs.Contains(userInput))
        {
            Console.WriteLine(invalidInputMsg);
            Console.WriteLine(prompt);
            userInput = Console.ReadLine();
        };
        return userInput;
    }
}