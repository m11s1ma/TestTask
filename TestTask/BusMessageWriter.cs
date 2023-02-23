using System.Buffers;
using System.Collections.Immutable;
using System.Text;

namespace TestTask
{
    public class BusMessageWriter
    {
        private readonly IBusConnection _connection = new BusConnection();
        private ImmutableList<byte> _buffer = ImmutableList.Create<byte>();
        private readonly int _bufferTrashOld = 1000;

        public async Task SendMessageAsync(byte[] nextMessage)
        {
            var temp = _buffer;
            ExecuteOperation(list => { return list.AddRange(nextMessage); });
            if (_buffer.Count() > _bufferTrashOld)
            {
                await _connection.PublishAsync(_buffer.ToArray());
                temp = _buffer;
                ExecuteOperation(list => { return list.Clear(); });
            }
        }

        private void ExecuteOperation(Func<ImmutableList<byte>, ImmutableList<byte>> operation)
        {
            ImmutableList<byte> original, temp;
            do
            {
                original = _buffer;
                temp = operation(original);
            }
            while (Interlocked.CompareExchange(ref _buffer, temp, original) != original) ;
        }
    }

    public interface IBusConnection
    {
        Task PublishAsync(byte[] nextMessage);
    }

    public class BusConnection : IBusConnection
    {
        public Task PublishAsync(byte[] nextMessage)
        {
            var ss = Encoding.UTF8.GetString(nextMessage);
            Console.WriteLine(ss);
            return Task.CompletedTask;
        }
    }
}