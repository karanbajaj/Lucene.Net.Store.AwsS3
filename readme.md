# Lucene.Net.Store.AwsS3 (Full Text Indexing for AWS S3 bucket)

## Project description
This project allows you to create Lucene Indexes via a Lucene Directory object which uses AWS S3 Bucket for persistent storage. 


## About

This is a fork of the [Lucene.Net.Store.Azure](https://github.com/tomlm/Lucene.Net.Store.Azure) by [Tom Laird-McConnell](https://github.com/tomlm)

This project allows you to create Lucene Indexes via a Lucene Directory object which uses AWS S3 Bucket for persistent storage. 

## Background
### Lucene.NET
**Lucene** is a mature Java based open source full text indexing and search engine and property store.

**Lucene.NET** is a mature port of that to C#

**Lucene/Lucene.Net** provides:
* Super simple API for storing documents with arbitrary properties 
* Complete control over what is indexed and what is stored for retrieval 
* Robust control over where and how things are indexed, how much memory is used, etc. 
* Superfast and super rich query capabilities 
* Sorted results 
* Rich constraint semantics AND/OR/NOT etc. 
* Rich text semantics (phrase match, wildcard match, near, fuzzy match etc) 
* Text query syntax (example: Title:(dog AND cat) OR Body:Lucen* ) 
* Programmatic expressions 
* Ranked results with custom ranking algorithms 
 
### AwsS3Directory class
**Lucene.Net.Store.AwsS3** implements a **Directory** storage provider called **AwsS3Directory** which smartly uses local file storage to cache files as they are created and automatically pushes them to blob storage as appropriate. Likewise, it smartly caches blob files back to the a client when they change. This provides with a nice blend of just in time syncing of data local to indexers or searchers across multiple machines.

With the flexibility that Lucene provides over data in memory versus storage and the just in time blob transfer that AwsS3Directory provides you have great control over the composibility of where data is indexed and how it is consumed.

To be more concrete: you can have 1..N worker roles adding documents to an index, and 1..N searcher webroles searching over the catalog in near real time.

## Usage

To use you need to create a AWS account and a AWS S3 Bucket.


To add documents to a catalog is as simple as
```c#
S3Settings settings = new S3Settings { BucketName = "", KeyID = "", SecretKey = "" };
AwsS3Directory awsDirectory = new AwsS3Directory(settings, "TestCatalog");
IndexWriter indexWriter = new IndexWriter(awsDirectory, new StandardAnalyzer(), true);
Document doc = new Document();
doc.Add(new Field("id", DateTime.Now.ToFileTimeUtc().ToString(), Field.Store.YES, Field.Index.TOKENIZED, Field.TermVector.NO));
doc.Add(new Field("Title", “this is my title”, Field.Store.YES, Field.Index.TOKENIZED, Field.TermVector.NO));
doc.Add(new Field("Body", “This is my body”, Field.Store.YES, Field.Index.TOKENIZED, Field.TermVector.NO));
indexWriter.AddDocument(doc);
indexWriter.Close();
```

And searching is as easy as:
```c#
IndexSearcher searcher = new IndexSearcher(awsDirectory);                
Lucene.Net.QueryParsers.QueryParser parser = QueryParser("Title", new StandardAnalyzer());
Lucene.Net.Search.Query query = parser.Parse("Title:(Dog AND Cat)");

Hits hits = searcher.Search(query);
for (int i = 0; i < hits.Length(); i++)
{
    Document doc = hits.Doc(i);
    Console.WriteLine(doc.GetField("Title").StringValue());
}
```

### Caching and Compression

AwsS3Directory compresses blobs before sent to the blob storage. Blobs are automatically cached local to reduce roundtrips for blobs which haven't changed. 

By default AwsS3Directory stores this local cache in a temporary folder. You can easily control where the local cache is stored by passing in a Directory object for whatever type and location of storage you want.

This example stores the cache in a ram directory:
```c#
CloudStorageAccount cloudStorageAccount = CloudStorageAccount.Parse("UseDevelopmentStorage=true");
AwsS3Directory awsDirectory = new AwsS3Directory(cloudStorageAccount, "MyIndex", new RAMDirectory());
```

And this example stores in the file system in C:\myindex
```c#
CloudStorageAccount cloudStorageAccount = CloudStorageAccount.Parse("UseDevelopmentStorage=true");
AwsS3Directory awsDirectory = new AwsS3Directory(cloudStorageAccount, "MyIndex", new FSDirectory(@"c:\myindex"));
```


## Notes on settings

Just like a normal lucene index, calling optimize too often causes a lot of churn and not calling it enough causes too many segment files to be created, so call it "just enough" times. That will totally depend on your application and the nature of your pattern of adding and updating items to determine (which is why lucene provides so many knobs to configure it's behavior).

The default compound file support that Lucene uses is to reduce the number of files that are generated...this means it deletes and merges files regularly which causes churn on the blob storage. Calling indexWriter.SetCompoundFiles(false) will give better performance, because more files means smaller blobs in blob storage and smaller network transactions because you only have to fetch new segments instead of merged segments.

We run it with a RAMDirectory for local cache and SetCompoundFiles(false); 

## Notes on locking

The AWS S3 does not provide locking mechanism. The locking is implemented using a separate lock file that writes a lease ID and expiry date. The AWS overwrites the object when multiple writes for same object is received. The last write wins.
The program first tries to see if the lock file is present. If present, it reads it and checks the lease ID and expiry date against the owner’s lease ID. Id IDs don’t match, that means the file is already locked by other process.
If no lock file is present, then the program tries to write the log file with it’s own lease ID and expiry date. (NOTE: if there are multiple concurrent writes, all succeed!). After writing, the program reads back the lock file and checks the load ID in that file. IF the lease ID match, then the lock is obtained. If they don’t match, that means the other concurrent write won the lock.

