using nietras.SeparatedValues;
using System.Data.SQLite;

// https://images.amazon.com/images/P/B01CKSRTQC.jpg

var kindleExportDirectory = @"D:\data\exports\Kindle\";
var year = 2024;

var relation = Path.Combine(kindleExportDirectory, @"Digital.SeriesContent.Relation.2/BookRelation.csv");
var content = Path.Combine(kindleExportDirectory, @"D:/data/exports/Kindle/Kindle.KindleContentUpdate/datasets/Kindle.KindleContentUpdate.ContentUpdates/Kindle.KindleContentUpdate.ContentUpdates.csv");
var whisper = Path.Combine(kindleExportDirectory, @"D:/data/exports/Kindle/Digital.Content.Whispersync/whispersync.csv");
var metadata = Path.Combine(kindleExportDirectory, @"D:/data/exports/Kindle/Kindle.KindleDocs/datasets/Kindle.KindleDocs.DocumentMetadata/Kindle.KindleDocs.DocumentMetadata.csv");

var readingSession = Path.Combine(kindleExportDirectory, @"Kindle.ReadingInsights/datasets/Kindle.reading-insights-sessions_with_adjustments/Kindle.reading-insights-sessions_with_adjustments.csv");

//TODO: Read config
var dbPath = "";

// Create database
//await CreateTables(dbPath);

var books = new List<(string id, string title)>();
// relation, e.g. B07XNB2XZX
{
    using var reader = Sep.Reader().FromFile(relation);
    foreach (var readRow in reader)
    {
        var id = readRow["ASIN"].ToString();
        var title = readRow["Product Name"].ToString();
        books.Add((id, title));
    }
}

// content
{
    using var reader = Sep.Reader().FromFile(content);
    foreach (var readRow in reader)
    {
        var id = readRow["ASIN"].ToString();
        var title = readRow["\"Product Name\""].ToString();
        if (books.Count(w => w.id == id) == 0)
            books.Add((id, title));
        if (books.First(w => w.id == id).title == "Not Available")
        {
            var f = books.First(w => w.id == id);
            books.Remove(f);
            f.title = title;
            books.Add(f);
        }
    }
}

// whisper
{
    using var reader = Sep.Reader().FromFile(whisper);
    foreach (var readRow in reader)
    {
        var id = readRow["ASIN"].ToString();
        var title = readRow["Product Name"].ToString();
        if (books.Count(w => w.id == id) == 0)
            books.Add((id, title));
        if (books.First(w => w.id == id).title == "Not Available")
        {
            var f = books.First(w => w.id == id);
            books.Remove(f);
            f.title = title;
            books.Add(f);
        }
    }
}

// metadata
{
    using var reader = Sep.Reader().FromFile(metadata);
    foreach (var readRow in reader)
    {
        var id = readRow["DocumentId"].ToString();
        var title = readRow["Title"].ToString();
        if (books.Count(w => w.id == id) == 0)
            books.Add((id, title));
        if (books.First(w => w.id == id).title == "Not Available")
        {
            var f = books.First(w => w.id == id);
            books.Remove(f);
            f.title = title;
            books.Add(f);
        }
    }
}

var notFound = new List<string>();
var readingEvents = new List<(string asin, string start, string end, string totalmilliseconds)>();
// reading session
{
    using var reader = Sep.Reader().FromFile(readingSession);
    foreach (var readRow in reader)
    {
        var id = readRow["ASIN"].ToString();
        var endTime = readRow["end_time"].ToString();
        var startTime = readRow["start_time"].ToString();
        var totalTime = readRow["total_reading_milliseconds"].ToString();

        if (books.Count(w => w.id == id) > 0)
        {
            readingEvents.Add((id, startTime, endTime, totalTime));
        }
        else
        {
            notFound.Add(id);
        }
    }
}

var distinct = notFound.Distinct();
foreach (var id in distinct)
{
    Console.WriteLine(id + " UNKNOWN");
}

foreach (var book in books.Distinct())
{
    var x = books.Distinct().Where(w => w.title.Contains("Frost"));
    var totalTime = readingEvents.Where(w => w.asin == book.id && Convert.ToDateTime(w.start) >= new DateTime(year, 1, 1) && Convert.ToDateTime(w.start) < new DateTime(year+1, 1, 1)).Sum(s => Convert.ToInt32(s.totalmilliseconds));
    if (totalTime > 0)
        Console.WriteLine($"{book.title} read for {(totalTime / 1000) / 60} minutes");
}


static async Task CreateTables(string dbPath)
{
    using var connection = new SQLiteConnection($"Data Source={dbPath};Version=3;");
    await connection.OpenAsync();

    var bookTableQuery = @"
                    CREATE TABLE IF NOT EXISTS Book (
                        Id INTEGER PRIMARY KEY AUTOINCREMENT,
                        ASIN TEXT NOT NULL,
                        Title TEXT NOT NULL,
                        Author TEXT NOT NULL,
                        UNIQUE (ASIN)
                    )";
    using var relationCommand = new SQLiteCommand(bookTableQuery, connection);
    await relationCommand.ExecuteNonQueryAsync();

    var readingSessionTableQuery = @"
                    CREATE TABLE IF NOT EXISTS ReadingSession (
                        Id INTEGER PRIMARY KEY AUTOINCREMENT,
                        BookId INTEGER NOT NULL,
                        StartTime DATETIME NOT NULL,
                        EndTime DATETIME NOT NULL,
                        Milliseconds INTEGER NOT NULL
                    )";
    using var readingSessionCommand = new SQLiteCommand(readingSessionTableQuery, connection);
    await readingSessionCommand.ExecuteNonQueryAsync();

    await connection.CloseAsync();
}

static async Task SaveBook(string dbPath)
{
    using var connection = new SQLiteConnection($"Data Source={dbPath};Version=3;");
    await connection.OpenAsync();

    var saveQuery = @"";

    using var command = new SQLiteCommand(saveQuery, connection);
    await command.ExecuteNonQueryAsync();

    await connection.CloseAsync();
}