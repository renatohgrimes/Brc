using System.Buffers.Text;
using System.Collections.Concurrent;
using System.Text;

const int EOF = -1;

#if DEBUG
args = [
    "3",
    "../../../measurements_1b.txt",
    "true"
];
#endif

var concurrentReaders = int.Parse(args.ElementAtOrDefault(0) ?? throw new ArgumentException("undefined concurrent readers"));

var filepath = args.ElementAtOrDefault(1) ?? throw new ArgumentException("undefined filepath");

var printProgress = bool.Parse(args.ElementAtOrDefault(2) ?? "false");

// compute chunks to divide file for multiple readers

var fs = File.OpenRead(filepath);

var chunks = new ReadableChunk[concurrentReaders];

var chunkSize = fs.Length / concurrentReaders;

for (var i = 0; i < chunks.Length; i++)
{
    var lastChunkEnd = i > 0 ? chunks[i - 1].End : -1;

    var start = lastChunkEnd + 1;
    var end = start + chunkSize;

    if (end > fs.Length - 1)
        end = fs.Length - 1;

    fs.Position = end;

    int read;
    for (; (read = fs.ReadByte()) is not '\n' and not EOF; end++) ;

    if (end > fs.Length - 1)
        end = fs.Length - 1;

    chunks[i] = new ReadableChunk(start, end);
}

fs.Close();
fs.Dispose();

// open multiple read streams

var streams = new FileStream[concurrentReaders];
for (int i = 0; i < streams.Length; i++)
    streams[i] = new FileStream(filepath, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize: 4096, useAsync: true);

// read file parallel by limiting each stream with chunks

var crc32 = new Crc32();
var results = new ConcurrentBag<(string, WeatherData)>();
var parallelOptions = new ParallelOptions { MaxDegreeOfParallelism = concurrentReaders };

Parallel.For(0, streams.Length, parallelOptions, parallelIndex =>
{
    var chunk = chunks[parallelIndex];
    var stream = streams[parallelIndex];

    long linesRead = 0;
    long bytesRead = 0;
    long bytes2Read = chunk.End - chunk.Start;

    Span<byte> buffer = stackalloc byte[32];
    var cities = new Dictionary<uint, string>();
    var weatherData = new Dictionary<uint, WeatherData>();

    stream.Position = chunk.Start;
    
    int read, pos;

    while (true)
    {
        for (pos = 0; (read = StreamReadByte()) is not ';' and not EOF; buffer[pos++] = (byte)read) ;

        var stopLoop = read == EOF || bytesRead >= bytes2Read;
        if (stopLoop)
        {
            foreach (var kv in cities)
            {
                var cityCrc = kv.Key;
                var cityStr = kv.Value;

                results.Add((cityStr, weatherData[cityCrc]));
            }

            break;
        }

        var crc = crc32.Compute(buffer[..pos]);

        if (!cities.TryGetValue(crc, out var city))
        {
            city = Encoding.UTF8.GetString(buffer[..pos]);
            cities.Add(crc, city);
        }

        for (pos = 0; (read = StreamReadByte()) is not '\n' and not EOF; buffer[pos++] = (byte)read) ;

        if (!Utf8Parser.TryParse(buffer[..pos], out double value, out _))
        {
            throw new FormatException($"worker {parallelIndex}: invalid format @ pos {stream.Position}");
        }

        if (!weatherData.TryGetValue(crc, out var data))
        {
            data = new WeatherData(value, value, value, 1);
        }
        else
        {
            data.Min = data.Min < value ? data.Min : value;
            data.Max = data.Max > value ? data.Max : value;
            data.Sum += value;
            data.Count += 1;
        }

        weatherData[crc] = data;

        linesRead++;

        if (printProgress && linesRead % 10_000_000 == 0)
            Console.WriteLine($"Thread: {parallelIndex} Lines: {linesRead:N0} Read: {bytesRead} 2Read: {bytes2Read}");
    }

    int StreamReadByte()
    {
        int b = stream.ReadByte();
        bytesRead++;
        return b;
    }
});

foreach (var stream in streams)
{
    stream.Close();
    stream.Dispose();        
}

// group, compute and print final results

var groups = results.GroupBy(x => x.Item1).OrderBy(x => x.Key).ToArray();

Console.Write('{');

for (int i = 0; i < groups.Length; i++)
{
    var grp = groups[i];

    var city = grp.Key;

    var min = grp.Select(x => x.Item2.Min).Min();
    var max = grp.Select(x => x.Item2.Max).Max();
    var sum = grp.Sum(x => x.Item2.Sum);
    var count = grp.Sum(x => x.Item2.Count);
    var avg = sum / count;

    Console.Write($"{city}={min:0.00}/{avg:0.00}/{max:0.00}");
    if (i < groups.Length - 1)
        Console.Write(", ");
}

Console.WriteLine('}');

/* ========== data structures ========= */

record struct WeatherData(double Min, double Max, double Sum, uint Count);

readonly record struct ReadableChunk(long Start, long End);

class Crc32
{
    private readonly uint[] _table;

    public Crc32()
    {
        _table = new uint[256];
        const uint polynomial = 0xEDB88320;

        for (uint i = 0; i < _table.Length; i++)
        {
            uint crc = i;
            for (int j = 0; j < 8; j++)
            {
                bool bit = (crc & 1) == 1;
                crc >>= 1;
                if (bit)
                    crc ^= polynomial;
            }
            _table[i] = crc;
        }
    }

    public uint Compute(ReadOnlySpan<byte> data)
    {
        uint crc = 0xFFFFFFFF;
        foreach (byte b in data)
        {
            byte index = (byte)((crc ^ b) & 0xFF);
            crc = (crc >> 8) ^ _table[index];
        }
        return ~crc;
    }
}
