using System.Collections.Concurrent;

static class WeatherWriter
{
    public static void Write(ConcurrentBag<(string, WeatherData)> values, TextWriter writer)
    {
        var groups = values.GroupBy(x => x.Item1).OrderBy(x => x.Key).ToArray();

        writer.Write('{');

        for (int i = 0; i < groups.Length; i++)
        {
            var grp = groups[i];

            var city = grp.Key;

            var min = grp.Select(x => x.Item2.Min).Min();
            var max = grp.Select(x => x.Item2.Max).Max();
            var sum = grp.Sum(x => x.Item2.Sum);
            var count = grp.Sum(x => x.Item2.Count);
            var avg = sum / count;

            writer.Write($"{city}={min:0.00}/{avg:0.00}/{max:0.00}");
            if (i < groups.Length - 1)
            {
                writer.Write(", ");
            }
        }

        writer.WriteLine('}');
    }
}
