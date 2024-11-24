using System;
using System.Collections.Generic;
using Microsoft.EntityFrameworkCore;

namespace AirportFlightScheduler.Data;

public partial class AirlineContext : DbContext
{
    public AirlineContext()
    {
    }

    public AirlineContext(DbContextOptions<AirlineContext> options)
        : base(options)
    {
    }

    public virtual DbSet<Airport> Airports { get; set; }

    public virtual DbSet<ConcessionPurchase> ConcessionPurchases { get; set; }

    public virtual DbSet<ConcessionPurchaseProduct> ConcessionPurchaseProducts { get; set; }

    public virtual DbSet<FlightHistory> FlightHistories { get; set; }

    public virtual DbSet<OverbookingRate> OverbookingRates { get; set; }

    public virtual DbSet<Passenger> Passengers { get; set; }

    public virtual DbSet<Payment> Payments { get; set; }

    public virtual DbSet<Plane> Planes { get; set; }

    public virtual DbSet<PlaneType> PlaneTypes { get; set; }

    public virtual DbSet<PlaneTypeSeatType> PlaneTypeSeatTypes { get; set; }

    public virtual DbSet<Product> Products { get; set; }

    public virtual DbSet<Reservation> Reservations { get; set; }

    public virtual DbSet<ScheduledFlight> ScheduledFlights { get; set; }

    public virtual DbSet<Seat> Seats { get; set; }

    public virtual DbSet<SeatType> SeatTypes { get; set; }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.HasPostgresExtension("btree_gist");

        modelBuilder.Entity<Airport>(entity =>
        {
            entity.HasKey(e => e.Id).HasName("airport_pkey");

            entity.ToTable("airport", "airline_booking2");

            entity.Property(e => e.Id)
                .UseIdentityAlwaysColumn()
                .HasColumnName("id");
            entity.Property(e => e.Address)
                .HasMaxLength(200)
                .HasColumnName("address");
            entity.Property(e => e.Code)
                .HasMaxLength(3)
                .HasColumnName("code");
        });

        modelBuilder.Entity<ConcessionPurchase>(entity =>
        {
            entity.HasKey(e => e.Id).HasName("concession_purchase_pkey");

            entity.ToTable("concession_purchase", "airline_booking2");

            entity.Property(e => e.Id)
                .UseIdentityAlwaysColumn()
                .HasColumnName("id");
            entity.Property(e => e.PaymentId).HasColumnName("payment_id");
            entity.Property(e => e.SeatId).HasColumnName("seat_id");

            entity.HasOne(d => d.Payment).WithMany(p => p.ConcessionPurchases)
                .HasForeignKey(d => d.PaymentId)
                .OnDelete(DeleteBehavior.ClientSetNull)
                .HasConstraintName("cp_payment_id");

            entity.HasOne(d => d.Seat).WithMany(p => p.ConcessionPurchases)
                .HasForeignKey(d => d.SeatId)
                .OnDelete(DeleteBehavior.ClientSetNull)
                .HasConstraintName("cp_seat_id");
        });

        modelBuilder.Entity<ConcessionPurchaseProduct>(entity =>
        {
            entity.HasKey(e => e.Id).HasName("concession_purchase_product_pkey");

            entity.ToTable("concession_purchase_product", "airline_booking2");

            entity.Property(e => e.Id)
                .UseIdentityAlwaysColumn()
                .HasColumnName("id");
            entity.Property(e => e.ConcessionPurchaseId).HasColumnName("concession_purchase_id");
            entity.Property(e => e.ProductId).HasColumnName("product_id");
            entity.Property(e => e.Quantity).HasColumnName("quantity");

            entity.HasOne(d => d.ConcessionPurchase).WithMany(p => p.ConcessionPurchaseProducts)
                .HasForeignKey(d => d.ConcessionPurchaseId)
                .OnDelete(DeleteBehavior.ClientSetNull)
                .HasConstraintName("fk_concession_purchase_id");

            entity.HasOne(d => d.Product).WithMany(p => p.ConcessionPurchaseProducts)
                .HasForeignKey(d => d.ProductId)
                .OnDelete(DeleteBehavior.ClientSetNull)
                .HasConstraintName("fk_product_id");
        });

        modelBuilder.Entity<FlightHistory>(entity =>
        {
            entity.HasKey(e => e.Id).HasName("flight_history_pkey");

            entity.ToTable("flight_history", "airline_booking2");

            entity.Property(e => e.Id)
                .UseIdentityAlwaysColumn()
                .HasColumnName("id");
            entity.Property(e => e.ActualArrivalTime)
                .HasColumnType("timestamp without time zone")
                .HasColumnName("actual_arrival_time");
            entity.Property(e => e.ActualDepartureTime)
                .HasColumnType("timestamp without time zone")
                .HasColumnName("actual_departure_time");
            entity.Property(e => e.PlaneId).HasColumnName("plane_id");
            entity.Property(e => e.ScheduledFlightId).HasColumnName("scheduled_flight_id");

            entity.HasOne(d => d.Plane).WithMany(p => p.FlightHistories)
                .HasForeignKey(d => d.PlaneId)
                .OnDelete(DeleteBehavior.ClientSetNull)
                .HasConstraintName("flight_history_plane_id_fkey");

            entity.HasOne(d => d.ScheduledFlight).WithMany(p => p.FlightHistories)
                .HasForeignKey(d => d.ScheduledFlightId)
                .OnDelete(DeleteBehavior.ClientSetNull)
                .HasConstraintName("flight_history_scheduled_flight_id_fkey");
        });

        modelBuilder.Entity<OverbookingRate>(entity =>
        {
            entity.HasKey(e => e.Id).HasName("overbooking_rate_pkey");

            entity.ToTable("overbooking_rate", "airline_booking2");

            entity.HasIndex(e => e.Rate, "overbooking_rate_rate_key").IsUnique();

            entity.Property(e => e.Id)
                .UseIdentityAlwaysColumn()
                .HasColumnName("id");
            entity.Property(e => e.Rate)
                .HasPrecision(5, 2)
                .HasColumnName("rate");
        });

        modelBuilder.Entity<Passenger>(entity =>
        {
            entity.HasKey(e => e.Id).HasName("passenger_pkey");

            entity.ToTable("passenger", "airline_booking2");

            entity.Property(e => e.Id)
                .UseIdentityAlwaysColumn()
                .HasColumnName("id");
            entity.Property(e => e.Address)
                .HasMaxLength(200)
                .HasColumnName("address");
            entity.Property(e => e.Email)
                .HasMaxLength(200)
                .HasColumnName("email");
            entity.Property(e => e.PassengerName)
                .HasMaxLength(100)
                .HasColumnName("passenger_name");
            entity.Property(e => e.PassportId)
                .HasMaxLength(9)
                .HasColumnName("passport_id");
            entity.Property(e => e.Phone)
                .HasMaxLength(15)
                .HasColumnName("phone");
        });

        modelBuilder.Entity<Payment>(entity =>
        {
            entity.HasKey(e => e.Id).HasName("payment_pkey");

            entity.ToTable("payment", "airline_booking2");

            entity.Property(e => e.Id)
                .UseIdentityAlwaysColumn()
                .HasColumnName("id");
            entity.Property(e => e.Amount)
                .HasPrecision(5, 2)
                .HasColumnName("amount");
            entity.Property(e => e.ReservationId).HasColumnName("reservation_id");

            entity.HasOne(d => d.Reservation).WithMany(p => p.Payments)
                .HasForeignKey(d => d.ReservationId)
                .OnDelete(DeleteBehavior.ClientSetNull)
                .HasConstraintName("fk_reservation_id");
        });

        modelBuilder.Entity<Plane>(entity =>
        {
            entity.HasKey(e => e.Id).HasName("plane_pkey");

            entity.ToTable("plane", "airline_booking2");

            entity.Property(e => e.Id)
                .UseIdentityAlwaysColumn()
                .HasColumnName("id");
            entity.Property(e => e.PlaneTypeId).HasColumnName("plane_type_id");

            entity.HasOne(d => d.PlaneType).WithMany(p => p.Planes)
                .HasForeignKey(d => d.PlaneTypeId)
                .OnDelete(DeleteBehavior.ClientSetNull)
                .HasConstraintName("plane_plane_type_id_fkey");
        });

        modelBuilder.Entity<PlaneType>(entity =>
        {
            entity.HasKey(e => e.Id).HasName("plane_type_pkey");

            entity.ToTable("plane_type", "airline_booking2");

            entity.HasIndex(e => e.PlaneName, "plane_type_plane_name_key").IsUnique();

            entity.Property(e => e.Id)
                .UseIdentityAlwaysColumn()
                .HasColumnName("id");
            entity.Property(e => e.PlaneName)
                .HasMaxLength(30)
                .HasColumnName("plane_name");
        });

        modelBuilder.Entity<PlaneTypeSeatType>(entity =>
        {
            entity.HasKey(e => e.Id).HasName("plane_type_seat_type_pkey");

            entity.ToTable("plane_type_seat_type", "airline_booking2");

            entity.Property(e => e.Id)
                .UseIdentityAlwaysColumn()
                .HasColumnName("id");
            entity.Property(e => e.PlaneTypeId).HasColumnName("plane_type_id");
            entity.Property(e => e.Quantity).HasColumnName("quantity");
            entity.Property(e => e.SeatTypeId).HasColumnName("seat_type_id");

            entity.HasOne(d => d.PlaneType).WithMany(p => p.PlaneTypeSeatTypes)
                .HasForeignKey(d => d.PlaneTypeId)
                .OnDelete(DeleteBehavior.ClientSetNull)
                .HasConstraintName("plane_type_seat_type_plane_type_id_fkey");

            entity.HasOne(d => d.SeatType).WithMany(p => p.PlaneTypeSeatTypes)
                .HasForeignKey(d => d.SeatTypeId)
                .OnDelete(DeleteBehavior.ClientSetNull)
                .HasConstraintName("plane_type_seat_type_seat_type_id_fkey");
        });

        modelBuilder.Entity<Product>(entity =>
        {
            entity.HasKey(e => e.Id).HasName("product_pkey");

            entity.ToTable("product", "airline_booking2");

            entity.HasIndex(e => e.ConcessionName, "product_concession_name_key").IsUnique();

            entity.Property(e => e.Id)
                .UseIdentityAlwaysColumn()
                .HasColumnName("id");
            entity.Property(e => e.ConcessionName)
                .HasMaxLength(200)
                .HasColumnName("concession_name");
            entity.Property(e => e.Price)
                .HasPrecision(5, 2)
                .HasColumnName("price");
        });

        modelBuilder.Entity<Reservation>(entity =>
        {
            entity.HasKey(e => e.Id).HasName("reservation_pkey");

            entity.ToTable("reservation", "airline_booking2");

            entity.Property(e => e.Id)
                .UseIdentityAlwaysColumn()
                .HasColumnName("id");
            entity.Property(e => e.PassengerId).HasColumnName("passenger_id");
            entity.Property(e => e.ScheduledFlightId).HasColumnName("scheduled_flight_id");
            entity.Property(e => e.SeatCount).HasColumnName("seat_count");
            entity.Property(e => e.SeatTypeId).HasColumnName("seat_type_id");
            entity.Property(e => e.TicketCost)
                .HasPrecision(5, 2)
                .HasColumnName("ticket_cost");

            entity.HasOne(d => d.Passenger).WithMany(p => p.Reservations)
                .HasForeignKey(d => d.PassengerId)
                .OnDelete(DeleteBehavior.ClientSetNull)
                .HasConstraintName("fk_passenger_id");

            entity.HasOne(d => d.ScheduledFlight).WithMany(p => p.Reservations)
                .HasForeignKey(d => d.ScheduledFlightId)
                .OnDelete(DeleteBehavior.ClientSetNull)
                .HasConstraintName("fk_scheduled_flight_id");

            entity.HasOne(d => d.SeatType).WithMany(p => p.Reservations)
                .HasForeignKey(d => d.SeatTypeId)
                .OnDelete(DeleteBehavior.ClientSetNull)
                .HasConstraintName("fk_seat_type_id");
        });

        modelBuilder.Entity<ScheduledFlight>(entity =>
        {
            entity.HasKey(e => e.Id).HasName("scheduled_flight_pkey");

            entity.ToTable("scheduled_flight", "airline_booking2");

            entity.Property(e => e.Id)
                .UseIdentityAlwaysColumn()
                .HasColumnName("id");
            entity.Property(e => e.ArrivalAirportId).HasColumnName("arrival_airport_id");
            entity.Property(e => e.ArrivalTime)
                .HasColumnType("timestamp without time zone")
                .HasColumnName("arrival_time");
            entity.Property(e => e.DepartureAirportId).HasColumnName("departure_airport_id");
            entity.Property(e => e.DepartureTime)
                .HasColumnType("timestamp without time zone")
                .HasColumnName("departure_time");
            entity.Property(e => e.OverbookingId).HasColumnName("overbooking_id");
            entity.Property(e => e.PlaneId).HasColumnName("plane_id");

            entity.HasOne(d => d.ArrivalAirport).WithMany(p => p.ScheduledFlightArrivalAirports)
                .HasForeignKey(d => d.ArrivalAirportId)
                .OnDelete(DeleteBehavior.ClientSetNull)
                .HasConstraintName("scheduled_flight_arrival_airport_id_fkey");

            entity.HasOne(d => d.DepartureAirport).WithMany(p => p.ScheduledFlightDepartureAirports)
                .HasForeignKey(d => d.DepartureAirportId)
                .OnDelete(DeleteBehavior.ClientSetNull)
                .HasConstraintName("scheduled_flight_departure_airport_id_fkey");

            entity.HasOne(d => d.Overbooking).WithMany(p => p.ScheduledFlights)
                .HasForeignKey(d => d.OverbookingId)
                .OnDelete(DeleteBehavior.ClientSetNull)
                .HasConstraintName("scheduled_flight_overbooking_id_fkey");

            entity.HasOne(d => d.Plane).WithMany(p => p.ScheduledFlights)
                .HasForeignKey(d => d.PlaneId)
                .OnDelete(DeleteBehavior.ClientSetNull)
                .HasConstraintName("scheduled_flight_plane_id_fkey");
        });

        modelBuilder.Entity<Seat>(entity =>
        {
            entity.HasKey(e => e.Id).HasName("seat_pkey");

            entity.ToTable("seat", "airline_booking2");

            entity.Property(e => e.Id)
                .UseIdentityAlwaysColumn()
                .HasColumnName("id");
            entity.Property(e => e.PassengerId).HasColumnName("passenger_id");
            entity.Property(e => e.PrintedBoardingPassAt)
                .HasColumnType("timestamp without time zone")
                .HasColumnName("printed_boarding_pass_at");
            entity.Property(e => e.ReservationId).HasColumnName("reservation_id");
            entity.Property(e => e.SeatNumber).HasColumnName("seat_number");

            entity.HasOne(d => d.Reservation).WithMany(p => p.Seats)
                .HasForeignKey(d => d.ReservationId)
                .OnDelete(DeleteBehavior.ClientSetNull)
                .HasConstraintName("fk_ab_reservation_id");
        });

        modelBuilder.Entity<SeatType>(entity =>
        {
            entity.HasKey(e => e.Id).HasName("seat_type_pkey");

            entity.ToTable("seat_type", "airline_booking2");

            entity.HasIndex(e => e.SeatType1, "seat_type_seat_type_key").IsUnique();

            entity.Property(e => e.Id)
                .UseIdentityAlwaysColumn()
                .HasColumnName("id");
            entity.Property(e => e.SeatType1)
                .HasMaxLength(15)
                .HasColumnName("seat_type");
        });

        OnModelCreatingPartial(modelBuilder);
    }

    partial void OnModelCreatingPartial(ModelBuilder modelBuilder);
}
