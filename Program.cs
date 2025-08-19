#if DEBUG
args = [
    "3",
    "../../../measurements.txt",
    "true"
];
#endif

var concurrentReaders = int.Parse(args.ElementAtOrDefault(0) ?? throw new ArgumentException("invalid concurrent readers"));
var filepath = args.ElementAtOrDefault(1) ?? throw new ArgumentException("invalid filepath");
var printProgress = bool.Parse(args.ElementAtOrDefault(2) ?? "false");

var chunks = FileChunks.Calculate(filepath, concurrentReaders);
var results = WeatherProcessor.Aggregate(filepath, concurrentReaders, printProgress, chunks);
WeatherWriter.Write(results, Console.Out);