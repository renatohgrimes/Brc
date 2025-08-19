static class FileChunks
{
    const int EOF = -1;

    public static FileChunk[] Calculate(string filepath, int concurrentReaders)
    {
        using var fs = File.OpenRead(filepath);

        var chunks = new FileChunk[concurrentReaders];

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

            chunks[i] = new FileChunk(start, end);
        }

        return chunks;
    }
}
