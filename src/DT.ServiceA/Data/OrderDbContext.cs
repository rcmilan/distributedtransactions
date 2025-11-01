using DT.ServiceA.Models;
using Microsoft.EntityFrameworkCore;

namespace DT.ServiceA.Data;

public class OrderDbContext : DbContext
{
    public OrderDbContext(DbContextOptions<OrderDbContext> options) : base(options)
    {

    }

    public DbSet<Order> Orders => Set<Order>();
    public DbSet<OutboxMessage> OutboxMessages => Set<OutboxMessage>();
    public DbSet<ProcessedMessage> ProcessedMessages => Set<ProcessedMessage>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);

        modelBuilder.Entity<Order>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.Property(x => x.Id).ValueGeneratedOnAdd();

            entity.Property(e => e.CustomerId).IsRequired();
            entity.Property(e => e.AmountInCents).IsRequired();
            entity.Property(e => e.Status).IsRequired();
            entity.Property(e => e.CreatedAt).IsRequired();

            entity.HasIndex(x => x.CreatedAt);
        });

        modelBuilder.Entity<OutboxMessage>(entity =>
        {
            entity.HasKey(x => x.Id);
            entity.Property(x => x.Id).ValueGeneratedOnAdd();

            entity.Property(x => x.EventType).IsRequired().HasMaxLength(256);
            entity.Property(x => x.Payload).IsRequired();

            // Index para consumer de não-processados
            entity.HasIndex(x => new { x.ProcessedAt, x.OccurredAt })
                .HasDatabaseName("idx_outbox_unprocessed")
                .IsUnique(false);
        });

        modelBuilder.Entity<ProcessedMessage>(entity =>
        {
            entity.HasKey(x => x.Id);
            entity.Property(x => x.Id).ValueGeneratedOnAdd();

            entity.Property(x => x.MessageId).IsRequired();
            entity.HasIndex(x => x.MessageId).IsUnique();

            entity.Property(x => x.ProcessedAt).IsRequired();
        });
    }
}
