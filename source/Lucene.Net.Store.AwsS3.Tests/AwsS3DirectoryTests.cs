using System;
using System.Diagnostics;
using System.Linq;
using System.Text;
using J2N.Collections.Generic;
using Lucene.Net.Analysis.Standard;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Util;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Lucene.Net.Store.AwsS3.Tests
{
	[TestClass]
	public class IntegrationTests
	{
		private static readonly S3Settings _settings = new S3Settings
		{
			BucketName = "",
			KeyID = "",
			SecretKey = ""
		};

		public IntegrationTests ()
		{
		}

		// Ensures index backward compatibility
		const LuceneVersion AppLuceneVersion = LuceneVersion.LUCENE_48;

		private static readonly string[] _authors = { "Paul Otto", "Cindi Bassford", "Dan Ongaro", "405(d) Task Group members" };
		private static readonly string[] _Caregories = { "405(d) Chronicles", "Task Group Articles", "HICP in the spotlight" };
		private static readonly string[] _resourceTypes = { "resource", "news", "post2" /*, "event2", "social" */ };
		private static readonly string[] _tags = { "Data Security", "Prevention", "Large ORganization", "Small Organization" };

		private Directory GetDirectoryHHS ()
		{
			const string catalogName = "testhhs";
			return new AwsS3Directory ( _settings, catalogName );

			//var basePath = Path.Combine ( Environment.ExpandEnvironmentVariables ( "%temp%" ), "AwsS3Directory" );
			//var indexPath = Path.Combine ( basePath, "local." + catalogName );
			//DirectoryInfo localDir = new DirectoryInfo ( indexPath );
			//if ( !localDir.Exists )
			//	localDir.Create ();

			//return FSDirectory.Open ( indexPath );
		}


		[TestMethod]
		public void Test_1_Write405d ()
		{
			using var awsDirectory = GetDirectoryHHS ();

			var indexWriterConfig = new IndexWriterConfig ( AppLuceneVersion, new StandardAnalyzer ( AppLuceneVersion ) )
			{
				OpenMode = OpenMode.CREATE_OR_APPEND
				//OpenMode = OpenMode.APPEND
			};

			using ( var indexWriter = new IndexWriter ( awsDirectory, indexWriterConfig ) )
			{
				for ( var i = 0; i < 50; i++ )
				{
					var id = Guid.NewGuid ().ToString ();


					var bodyText = GeneratePhrase ( 40 );
					var doc = new Document {
						new StoredField("xid", id),
						new StoredField("date", DateTime.UtcNow.ToString("O")),

						new StringField("type", _resourceTypes[Random.Next (_resourceTypes.Length )], Field.Store.YES ),
						new TextField("category", _Caregories[Random.Next (_Caregories.Length )], Field.Store.YES ),

						new TextField("author", _authors[Random.Next(_authors.Length)],Field.Store.YES),
						new TextField("title", GeneratePhrase(10), Field.Store.YES),
						new TextField("descr", GeneratePhrase(20), Field.Store.YES),
						new TextField("content", bodyText, Field.Store.NO)
					};

					var nbrTags = Random.Next ( 1, 4 );

					var tags = new HashSet<string> ();
					while ( tags.Count < nbrTags )
					{
						tags.Add ( _tags[Random.Next ( _tags.Length )] );
					}

					foreach ( var tag in tags )
					{
						doc.Add ( new TextField ( "tags", tag, Field.Store.YES ) );
					}

					indexWriter.AddDocument ( doc );
				}
				indexWriter.Flush ( triggerMerge: true, applyAllDeletes: true );
				indexWriter.Commit ();

				indexWriter.Dispose ();
			}

			GC.Collect ();
		}

		[TestMethod]
		public void Test_2_Read405d ()
		{
			using var awsDirectory = GetDirectoryHHS ();

			try
			{
				using var reader = DirectoryReader.Open ( awsDirectory );
				var searcher = new IndexSearcher ( reader );
				var parserC = new QueryParsers.Classic.QueryParser ( AppLuceneVersion, "content", new StandardAnalyzer ( AppLuceneVersion ) );
				var parserT = new QueryParsers.Classic.QueryParser ( AppLuceneVersion, "title", new StandardAnalyzer ( AppLuceneVersion ) );
				var parserD = new QueryParsers.Classic.QueryParser ( AppLuceneVersion, "descr", new StandardAnalyzer ( AppLuceneVersion ) );
				var parserA = new QueryParsers.Classic.QueryParser ( AppLuceneVersion, "author", new StandardAnalyzer ( AppLuceneVersion ) );

				var parserK = new QueryParsers.Classic.QueryParser ( AppLuceneVersion, "category", new StandardAnalyzer ( AppLuceneVersion ) );
				var parserG = new QueryParsers.Classic.QueryParser ( AppLuceneVersion, "tags", new StandardAnalyzer ( AppLuceneVersion ) );

				// First show counts...
				foreach ( var type in _resourceTypes )
				{
					var topDocs = searcher.Search ( new TermQuery ( new Term ( "type", type ) ), 1 );

					Console.WriteLine ( "{0,-15} : {1,4}", type, topDocs.TotalHits );
				}
				Console.WriteLine ( "================================" );

				var threshold = 0.02f;

				var searchTerm = _tags[0];

				var topDocsx = searcher.Search ( parserG.Parse ( searchTerm ), 2 );

				if ( topDocsx.TotalHits > 0 )
				{
					var docx = searcher.Doc ( topDocsx.ScoreDocs[0].Doc );
					foreach ( var f in docx.Fields )
					{
						var val = docx.Get ( f.Name );
					}

				}

				foreach ( var type in _resourceTypes )
				{
					var query = new BooleanQuery
					{
						{ new TermQuery (new Term ( "type", type)) {Boost=0.01f}, Occur.MUST },
						{ parserA.Parse(searchTerm), Occur.SHOULD },
						{ parserC.Parse(searchTerm), Occur.SHOULD },
						{ parserD.Parse(searchTerm), Occur.SHOULD },
						{ parserT.Parse(searchTerm), Occur.SHOULD },
						{ GetBoostedQuery ( parserK.Parse(searchTerm), 2.0f), Occur.SHOULD },
						{ GetBoostedQuery ( parserG.Parse(searchTerm), 2.0f), Occur.SHOULD },
					};

					var topDocs = searcher.Search ( query, 100 );
					var docs = topDocs.ScoreDocs.Where ( y => y.Score >= topDocs.MaxScore * threshold ).ToList ();

					var flag = ( docs.Count == topDocs.ScoreDocs.Length && docs.Count < topDocs.TotalHits ) ? "*" : "";

					Console.WriteLine ( "Found {0}{4} ({3}) docs for {1}/{2}", docs.Count, type, searchTerm, topDocs.MaxScore, flag );
					for ( int idx = 0; idx < Math.Min ( 5, docs.Count ); idx++ )
					{
						var doc = searcher.Doc ( docs[idx].Doc );

						//	var xid = doc.Get ( "xid" );
						var author = doc.Get ( "author" );
						var title = doc.Get ( "title" );
						var tags = doc.GetValues ( "tags" );
						Console.WriteLine ( "{4,6:#.##%}|{5:0.0000}|{0}: {3} : ({1}){2}", title, tags.Length, string.Join ( ", ", tags ), author,
						docs[idx].Score / topDocs.MaxScore, docs[idx].Score );
					}
					Console.WriteLine ();
				}
				Trace.TraceInformation ( "Tests passsed" );
			}
			catch ( Exception x )
			{
				Trace.TraceInformation ( "Tests failed:\n{0}", x );
				throw;
			}

			static Query GetBoostedQuery ( Query query, float boost )
			{
				query.Boost = boost;
				return query;
			}
		}



		[TestMethod]
		public void TestReadAndWriteMB ()
		{
			const string catalogName = "testcatalogmb";

			var awsDirectory = new AwsS3Directory ( _settings, catalogName );

			InitializeCatalogMB ( awsDirectory, 1000 );

			try
			{

				var ireader = DirectoryReader.Open ( awsDirectory );
				for ( var i = 0; i < 100; i++ )
				{
					var searcher = new IndexSearcher ( ireader );
					var parser = new QueryParsers.Classic.QueryParser ( AppLuceneVersion, "Body", new StandardAnalyzer ( AppLuceneVersion ) );
					var query = parser.Parse ( "Body:cat Title:dog" );

					var q = new BooleanQuery
					{
						{ new TermQuery ( new Term ( "Team", "eagles" ) ), Occur.MUST },
						{ query, Occur.MUST }
					};
					var topDocs = searcher.Search ( q, 100 );

					//query = parser.Parse ( "+Team:Eagles +cat" );
					//topDocs = searcher.Search ( query, 100 );

					for ( int idx = 0; idx < Math.Min ( 5, topDocs.TotalHits ); idx++ )
					{
						var doc = searcher.Doc ( topDocs.ScoreDocs[idx].Doc );

						var title = doc.Get ( "Title" );
						var teams = doc.GetValues ( "Team" );
						Console.WriteLine ( "{0}: ({1}){2}", title, teams.Length, string.Join ( ", ", teams ) );
					}
					Console.WriteLine ();
				}
				Trace.TraceInformation ( "Tests passsed" );
			}
			catch ( Exception x )
			{
				Trace.TraceInformation ( "Tests failed:\n{0}", x );
			}
			finally
			{
				// check the container exists, and delete it
			}
		}

		private static void InitializeCatalogMB ( AwsS3Directory awsDirectory, int docs )
		{
			var indexWriterConfig = new IndexWriterConfig (
				  AppLuceneVersion,
				  new StandardAnalyzer ( AppLuceneVersion ) );

			var dog = 0;
			var cat = 0;
			var car = 0;
			using ( var indexWriter = new IndexWriter ( awsDirectory, indexWriterConfig ) )
			{

				for ( var iDoc = 0; iDoc < docs; iDoc++ )
				{
					var bodyText = GeneratePhrase ( 40 );
					var doc = new Document {
						new TextField("id", DateTime.Now.ToFileTimeUtc() + "-" + iDoc, Field.Store.YES),
						new TextField("Title", GeneratePhrase(10), Field.Store.YES),
						new TextField("Body", bodyText, Field.Store.YES)
					};

					var nbrTeams = Random.Next ( 2, 4 );

					var teams = new HashSet<string> ();
					while ( teams.Count < nbrTeams )
					{
						teams.Add ( SampleTeams[Random.Next ( SampleTeams.Length )] );
					}

					foreach ( var team in teams )
					{
						doc.Add ( new TextField ( "Team", team, Field.Store.YES ) );
					}

					dog += bodyText.Contains ( " dog " ) ? 1 : 0;
					cat += bodyText.Contains ( " cat " ) ? 1 : 0;
					car += bodyText.Contains ( " car " ) ? 1 : 0;
					indexWriter.AddDocument ( doc );
				}

				indexWriter.Flush ( triggerMerge: false, applyAllDeletes: false );


				Console.WriteLine ( "Total docs is {0}, {1} dog, {2} cat, {3} car", indexWriter.NumDocs, dog, cat, car );
				Trace.TraceInformation ( "Total docs is {0}, {1} dog, {2} cat, {3} car", indexWriter.NumDocs, dog, cat, car );
			}

			return;
		}

		private static readonly string[] SampleTeams = { "Eagles", "Cowboys", "Ravens", "Falcons", "Giants", "Jets" };


		[TestMethod]
		public void TestReadAndWrite ()
		{
			const string catalogName = "testcatalog";

			var awsDirectory = new AwsS3Directory ( _settings, catalogName );

			var (dog, cat, car) = InitializeCatalog ( awsDirectory, 1000 );
			Console.WriteLine ( $"dog:{dog}, cat:{cat}, car:{car}." );

			try
			{
				var ireader = DirectoryReader.Open ( awsDirectory );
				for ( var i = 0; i < 100; i++ )
				{
					var searcher = new IndexSearcher ( ireader );
					var searchForPhrase = SearchForPhrase ( searcher, "dog" );
					Assert.AreEqual ( dog, searchForPhrase );
					searchForPhrase = SearchForPhrase ( searcher, "cat" );
					Assert.AreEqual ( cat, searchForPhrase );
					searchForPhrase = SearchForPhrase ( searcher, "car" );
					Assert.AreEqual ( car, searchForPhrase );
				}
				Trace.TraceInformation ( "Tests passsed" );
			}
			catch ( Exception x )
			{
				Trace.TraceInformation ( "Tests failed:\n{0}", x );
			}
			finally
			{
				// delete all directory blobs
				//foreach ( string file in awsDirectory.ListAll () )
				//{
				//	awsDirectory.DeleteFile ( file );
				//}
			}
		}

		[TestMethod]
		public void TestReadAndWriteWithSubDirectory ()
		{
			const string containerName = "testcatalogwithshards";

			var directory1 = new AwsS3Directory ( _settings, $"{containerName}/shard1" );
			var (dog, cat, car) = InitializeCatalog ( directory1, 1000 );
			var directory2 = new AwsS3Directory ( _settings, $"{containerName}/shard2" );
			var (dog2, cat2, car2) = InitializeCatalog ( directory2, 500 );

			ValidateDirectory ( directory1, dog, cat, car );
			ValidateDirectory ( directory2, dog2, cat2, car2 );

			// delete all directory1 blobs
			foreach ( string file in directory1.ListAll () )
			{
				directory1.DeleteFile ( file );
			}

			ValidateDirectory ( directory2, dog2, cat2, car2 );

			foreach ( string file in directory2.ListAll () )
			{
				directory2.DeleteFile ( file );
			}
		}

		private static void ValidateDirectory ( AwsS3Directory directory, Int32 dog2, Int32 cat2, Int32 car2 )
		{
			var ireader = DirectoryReader.Open ( directory );
			for ( var i = 0; i < 100; i++ )
			{
				var searcher = new IndexSearcher ( ireader );
				var searchForPhrase = SearchForPhrase ( searcher, "dog" );
				Assert.AreEqual ( dog2, searchForPhrase );
				searchForPhrase = SearchForPhrase ( searcher, "cat" );
				Assert.AreEqual ( cat2, searchForPhrase );
				searchForPhrase = SearchForPhrase ( searcher, "car" );
				Assert.AreEqual ( car2, searchForPhrase );
			}
			Trace.TraceInformation ( "Tests passsed" );
		}

		private static (int dog, int cat, int car) InitializeCatalog ( AwsS3Directory awsDirectory, int docs )
		{
			var indexWriterConfig = new IndexWriterConfig ( AppLuceneVersion, new StandardAnalyzer ( AppLuceneVersion ) );

			var dog = 0;
			var cat = 0;
			var car = 0;
			using ( var indexWriter = new IndexWriter ( awsDirectory, indexWriterConfig ) )
			{
				for ( var iDoc = 0; iDoc < docs; iDoc++ )
				{
					var bodyText = GeneratePhrase ( 40 );
					var doc = new Document {
						new TextField("id", DateTime.Now.ToFileTimeUtc() + "-" + iDoc, Field.Store.YES),
						new TextField("Title", GeneratePhrase(10), Field.Store.YES),
						new TextField("Body", bodyText, Field.Store.YES)
					};
					dog += bodyText.Contains ( " dog " ) ? 1 : 0;
					cat += bodyText.Contains ( " cat " ) ? 1 : 0;
					car += bodyText.Contains ( " car " ) ? 1 : 0;
					indexWriter.AddDocument ( doc );
				}

				indexWriter.Flush ( triggerMerge: false, applyAllDeletes: false );

				Console.WriteLine ( "Total docs is {0}, {1} dog, {2} cat, {3} car", indexWriter.NumDocs, dog, cat, car );
				Trace.TraceInformation ( "Total docs is {0}, {1} dog, {2} cat, {3} car", indexWriter.NumDocs, dog, cat, car );
			}

			return (dog, cat, car);
		}

		private static int SearchForPhrase ( IndexSearcher searcher, string phrase )
		{
			var parser = new QueryParsers.Classic.QueryParser ( AppLuceneVersion, "Body", new StandardAnalyzer ( AppLuceneVersion ) );
			var query = parser.Parse ( phrase );
			var topDocs = searcher.Search ( query, 100 );
			return topDocs.TotalHits;
		}

		private static readonly Random Random = new Random ();

		private static readonly string[] SampleTerms = {
			"dog", "cat", "car", "horse", "door", "tree", "chair", "microsoft", "apple", "adobe", "google", "golf",
			"linux", "windows", "firefox", "mouse", "hornet", "monkey", "giraffe", "computer", "monitor",
			"steve", "fred", "lili", "albert", "tom", "shane", "gerald", "chris",
			"love", "hate", "scared", "fast", "slow", "new", "old"
		};

		private static string GeneratePhrase ( int maxTerms )
		{
			var phrase = new StringBuilder ();
			var nWords = 2 + Random.Next ( maxTerms );
			for ( var i = 0; i < nWords; i++ )
			{
				phrase.AppendFormat ( " {0} {1}", SampleTerms[Random.Next ( SampleTerms.Length )],
									Random.Next ( 32768 ).ToString () );
			}
			return phrase.ToString ();
		}
	}
}
