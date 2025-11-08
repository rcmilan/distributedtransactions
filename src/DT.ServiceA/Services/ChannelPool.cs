using RabbitMQ.Client;
using System.Collections.Concurrent;

namespace DT.ServiceA.Services;

/// <summary>
/// A thread-safe pool for managing RabbitMQ channels efficiently.
/// Channels are reused to improve performance and reduce overhead.
/// </summary>
/// <remarks>
/// Initializes a new instance of the ChannelPool with the specified RabbitMQ connection.
/// </remarks>
/// <param name="connection">The RabbitMQ connection to create channels from.</param>
public class ChannelPool(IConnection connection) : IDisposable
{
    private readonly ConcurrentQueue<IChannel> _availableChannels = new();
    private readonly IConnection _connection = connection ?? throw new ArgumentNullException(nameof(connection));
    private bool _disposed;

    /// <summary>
    /// Gets an available channel from the pool, or creates a new one if none are available.
    /// </summary>
    /// <param name="cancellationToken">A cancellation token to cancel the operation.</param>
    /// <returns>An IChannel instance.</returns>
    /// <exception cref="ObjectDisposedException">Thrown if the pool has been disposed.</exception>
    public async Task<IChannel> GetChannelAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, nameof(ChannelPool));

        return _availableChannels.TryDequeue(out var channel) ? channel
            : await _connection.CreateChannelAsync(cancellationToken: cancellationToken);
    }

    /// <summary>
    /// Returns a channel to the pool for reuse.
    /// </summary>
    /// <param name="channel">The channel to return.</param>
    public void ReturnChannel(IChannel channel)
    {
        if (_disposed || channel == null)
            return;

        _availableChannels.Enqueue(channel);
    }

    /// <summary>
    /// Disposes the pool and all pooled channels.
    /// </summary>
    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;

        while (_availableChannels.TryDequeue(out var channel))
        {
            channel.Dispose();
        }

        GC.SuppressFinalize(this);
    }
}