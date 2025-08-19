using System.Collections.Concurrent;
using System.Text;

static class WeatherProcessor
{
    const int EOF = -1;
    const int StreamBufferSize = 4 * 1024 * 1024;

    public static ConcurrentBag<(string, WeatherData)> Aggregate(
        string filepath,
        int concurrentReaders,
        bool printProgress,
        FileChunk[] chunks)
    {
        var results = new ConcurrentBag<(string, WeatherData)>();
        var parallelOptions = new ParallelOptions { MaxDegreeOfParallelism = concurrentReaders };

        var streams = new FileStream[concurrentReaders];
        for (int i = 0; i < streams.Length; i++)
            streams[i] = new FileStream(filepath, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize: StreamBufferSize);

        Parallel.For(0, streams.Length, parallelOptions, parallelIndex =>
        {
            var chunk = chunks[parallelIndex];
            var stream = streams[parallelIndex];

            long linesRead = 0;
            long bytesRead = 0;
            long bytes2Read = chunk.End - chunk.Start;

            var streamBuffer = new byte[StreamBufferSize];

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

                var crc = Crc32.Compute(buffer[..pos]);

                if (!cities.ContainsKey(crc))
                {
                    var city = Encoding.UTF8.GetString(buffer[..pos]);
                    cities.Add(crc, city);
                }

                for (pos = 0; (read = StreamReadByte()) is not '\n' and not EOF; buffer[pos++] = (byte)read) ;

                if (!TemperatureParser.TryParse(buffer[..pos], out float value))
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
                var ringPos = bytesRead % StreamBufferSize;
                if (ringPos == 0)
                {
                    stream.Read(streamBuffer);
                }

                int b = streamBuffer[ringPos];

                bytesRead++;

                return b;
            }
        });

        foreach (var stream in streams)
        {
            stream.Close();
            stream.Dispose();
        }

        return results;
    }
}
