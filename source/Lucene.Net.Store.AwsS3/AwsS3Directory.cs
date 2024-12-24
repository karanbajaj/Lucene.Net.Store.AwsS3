//    License: Microsoft Public License (Ms-PL) 
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Runtime;
using Amazon;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;

namespace Lucene.Net.Store.AwsS3
{
	public class S3Settings
	{
        public string ConnectionString { get; set; }
       
        public string AccessKey { get; set; }
		public string SecretKey { get; set; }
        public AWSCredentials Credentials { get; set; }
        public string Region { get; set; }
        public string ServiceUrl { get; set; }
        public bool? UseChunkEncoding { get; set; }
        public S3CannedACL CannedACL { get; set; }
        public string BucketName { get; set; }
        public string BucketFolder { get; set; }

        public AWSCredentials GetCredentials()
        {
            return !String.IsNullOrEmpty(AccessKey)
                ? new BasicAWSCredentials(AccessKey, SecretKey)
                : null;
        }

        public RegionEndpoint GetRegion()
        {
            return !String.IsNullOrEmpty(Region)
                ? RegionEndpoint.GetBySystemName(Region)
                : null;
        }
        public virtual bool ParseItem(string key, string value)
        {
            if (String.Equals(key, "AccessKey", StringComparison.OrdinalIgnoreCase) ||
                String.Equals(key, "Access Key", StringComparison.OrdinalIgnoreCase) ||
                String.Equals(key, "AccessKeyId", StringComparison.OrdinalIgnoreCase) ||
                String.Equals(key, "Access Key Id", StringComparison.OrdinalIgnoreCase) ||
                String.Equals(key, "Id", StringComparison.OrdinalIgnoreCase))
            {
                AccessKey = value;
                return true;
            }
            if (String.Equals(key, "SecretKey", StringComparison.OrdinalIgnoreCase) ||
                String.Equals(key, "Secret Key", StringComparison.OrdinalIgnoreCase) ||
                String.Equals(key, "SecretAccessKey", StringComparison.OrdinalIgnoreCase) ||
                String.Equals(key, "Secret Access Key", StringComparison.OrdinalIgnoreCase) ||
                String.Equals(key, "Secret", StringComparison.OrdinalIgnoreCase))
            {
                SecretKey = value;
                return true;
            }
            if (String.Equals(key, "EndPoint", StringComparison.OrdinalIgnoreCase) ||
                String.Equals(key, "End Point", StringComparison.OrdinalIgnoreCase) ||
                String.Equals(key, "Region", StringComparison.OrdinalIgnoreCase))
            {
                Region = value;
                return true;
            }
            if (String.Equals(key, "Service", StringComparison.OrdinalIgnoreCase) ||
                String.Equals(key, "Service Url", StringComparison.OrdinalIgnoreCase) ||
                String.Equals(key, "ServiceUrl", StringComparison.OrdinalIgnoreCase))
            {
                ServiceUrl = value;
                return true;
            }
            if (String.Equals(key, "Bucket", StringComparison.OrdinalIgnoreCase) ||
                String.Equals(key, "Bucket Name", StringComparison.OrdinalIgnoreCase) )
            {
                ServiceUrl = value;
                return true;
            }
            return false;
        }

        public override string ToString()
        {
            string connectionString = String.Empty;
            if (!String.IsNullOrEmpty(AccessKey))
                connectionString += "AccessKey=" + AccessKey + ";";
            if (!String.IsNullOrEmpty(SecretKey))
                connectionString += "SecretKey=" + SecretKey + ";";
            if (!String.IsNullOrEmpty(Region))
                connectionString += "Region=" + Region + ";";
            if (!String.IsNullOrEmpty(ServiceUrl))
                connectionString += "ServiceUrl=" + ServiceUrl + ";";
            return connectionString;
        }
    }

	public class S3SettingBuilder
    {
        private readonly S3Settings _settings = new S3Settings();

        public S3SettingBuilder ConnectionString(string connectionString)
        {
            if (String.IsNullOrEmpty(connectionString))
                throw new ArgumentNullException(nameof(connectionString));
            _settings.ConnectionString = connectionString;
            return this;
        }
        public S3SettingBuilder BucketName(string bucketName)
        {
            _settings.BucketName = bucketName;
            return this;
        }
		public S3SettingBuilder AccessKey(string accessKey)
        {
            _settings.AccessKey = accessKey;
            return this;
        }
        public S3SettingBuilder SecretKey(string secretKey)
        {
            _settings.SecretKey = secretKey;
            return this;
        }

        public S3SettingBuilder BucketFolder(string bucketFolder)
        {
            _settings.BucketFolder = bucketFolder;
            return this;
        }

        public S3SettingBuilder Credentials(AWSCredentials credentials)
        {
            _settings.Credentials = credentials;
            return this;
        }
        public S3SettingBuilder Credentials(string accessKey, string secretKey)
        {
            if (String.IsNullOrEmpty(accessKey))
                throw new ArgumentNullException(nameof(accessKey));
            if (String.IsNullOrEmpty(secretKey))
                throw new ArgumentNullException(nameof(secretKey));

            _settings.Credentials = new BasicAWSCredentials(accessKey, secretKey);
            return this;
        }

        public S3SettingBuilder Region(RegionEndpoint region)
        {
            _settings.Region = region.ToString();
            return this;
        }

        public S3SettingBuilder ServiceUrl(string serviceUrl)
        {
            _settings.ServiceUrl = serviceUrl;
            return this;
        }

        public S3SettingBuilder UseChunkEncoding(bool useChunkEncoding)
        {
            _settings.UseChunkEncoding = useChunkEncoding;
            return this;
        }

        public S3SettingBuilder CannedACL(S3CannedACL cannedACL)
        {
            _settings.CannedACL = cannedACL;
            return this;
        }
        public S3SettingBuilder CannedACL(string cannedAcl)
        {
            if (String.IsNullOrEmpty(cannedAcl))
                throw new ArgumentNullException(nameof(cannedAcl));
            _settings.CannedACL = S3CannedACL.FindValue(cannedAcl);
            return this;
        }

        public S3Settings Build()
        {
            if (String.IsNullOrEmpty(_settings.ConnectionString))
                return _settings;

            Parse(_settings.ConnectionString);

            return _settings;
        }
        private void Parse(string connectionString)
        {
            foreach (var option in connectionString
                .Split(new[] { ';' }, StringSplitOptions.RemoveEmptyEntries)
                .Where(kvp => kvp.Contains('='))
                .Select(kvp => kvp.Split(new[] { '=' }, 2)))
            {
                string optionKey = option[0].Trim();
                string optionValue = option[1].Trim();
                if (!_settings.ParseItem(optionKey, optionValue))
                    throw new ArgumentException($"The option '{optionKey}' cannot be recognized in connection string.", nameof(connectionString));
            }
        }
      
    }

    public class AwsS3Directory : Directory
	{
		private readonly string _subDirectory;

		private readonly Dictionary<string, AwsS3Lock> _locks = new Dictionary<string, AwsS3Lock> ();
		private LockFactory _lockFactory = new NativeFSLockFactory ();
		private readonly Dictionary<string, AwsS3IndexOutput> _nameCache = new Dictionary<string, AwsS3IndexOutput> ();

		public AmazonS3Client S3Client { get; private set; }
		public string Name { get; set; }
		public string BucketName { get; private set; }

		public override LockFactory LockFactory => _lockFactory;


		/// <summary>
		/// If set, this is the directory object to use as the local cache
		/// </summary>
		public Directory CacheDirectory { get; set; }

		public AwsS3Directory ( S3Settings settings ) :
				this ( settings, null, null )
		{
		}

		/// <summary>
		/// Create AwsS3Directory
		/// </summary>
		/// <param name="storageAccount">staorage account to use</param>
		/// <param name="catalog">name of catalog (folder in blob storage, can have subfolders like foo/bar)</param>
		/// <remarks>Default local cache is to use file system in user/appdata/AwsS3Directory/Catalog</remarks>
		public AwsS3Directory ( S3Settings settings, string catalog )
			: this ( settings, catalog, null )
		{
		}

		/// <summary>
		/// Create an AwsS3Directory
		/// </summary>
		/// <param name="storageAccount">storage account to use</param>
		/// <param name="catalog">name of catalog (folder in blob storage, can have subfolders like foo/bar)</param>
		/// <param name="cacheDirectory">local Directory object to use for local cache</param>
		public AwsS3Directory ( S3Settings settings, string catalog, Directory cacheDirectory )
		{
			if ( settings == null )
				throw new ArgumentNullException ( nameof ( settings ) );

			if ( string.IsNullOrEmpty ( catalog ) )
				Name = "lucene";
			else
				Name = catalog.Trim ();

			var list = ( settings.BucketFolder + "/" + Name )
				.Split ( '/' )
				.Where ( x => !string.IsNullOrWhiteSpace ( x ) )
				.Select ( y => y.Trim () );

			_subDirectory = string.Join ( "/", list );

			S3Client = GetClient ( settings );
			BucketName = settings.BucketName;

			InitCacheDirectory ( cacheDirectory );
		}

		private static AmazonS3Client GetClient ( S3Settings settings )
		{
			if ( string.IsNullOrEmpty ( settings.AccessKey ) && string.IsNullOrEmpty ( settings.SecretKey ) )
				return new AmazonS3Client ();

			var client = new AmazonS3Client(settings.GetCredentials(), settings.GetRegion()); 
			return client;
		}

		public void ClearCache ()
		{
			if ( CacheDirectory == null )
				return;

			foreach ( string file in CacheDirectory.ListAll () )
			{
				CacheDirectory.DeleteFile ( file );
			}
		}

		#region DIRECTORYMETHODS

		/// <summary>Returns an array of strings, one for each file in the directory. </summary>
		public override string[] ListAll ()
		{
			var request = new ListObjectsRequest
			{
				BucketName = BucketName,
				Prefix = _subDirectory + "/"
			};

			var result = new List<string> ();

			try
			{
				do
				{
					var response = S3Client.ListObjectsAsync ( request ).GetAwaiter ().GetResult ();

					// Process response.
					result.AddRange ( response.S3Objects.Select ( y => y.Key.Substring ( request.Prefix.Length ) ) );

					// If response is truncated, set the marker to get the next 
					// set of keys.
					if ( response.IsTruncated )
						request.Marker = response.NextMarker;
					else
						request = null;
				} while ( request != null );

				return result.ToArray ();
			}
			catch ( Exception )
			{
			}

			return new string[0];
		}

		/// <summary>Returns true if a file with the given name exists. </summary>
		[Obsolete ( "this method will be removed in 5.0" )]
		public override bool FileExists ( string name )
		{
			// this always comes from the server
			var request = new GetObjectRequest
			{
				BucketName = BucketName,
				Key = GetBlobName ( name )
			};

			try
			{
				using ( var response = S3Client.GetObjectAsync ( request ).GetAwaiter ().GetResult () )
				{
					using ( var responseStream = response.ResponseStream )
					{
						if ( responseStream != null )
							return true;
					}
				}
			}
			catch ( Exception )
			{
				return false;
			}
			return false;
		}

		/// <summary>Removes an existing file in the directory. </summary>
		public override void DeleteFile ( string name )
		{
#if FULLDEBUG
			Debug.WriteLine ( $"{Name} deleting {name} " );
#endif

			string key;
			if ( _subDirectory.Length > 1 && name.StartsWith ( $"{_subDirectory}/" ) )
				key = name;
			else
				key = GetBlobName ( name );

			var request = new DeleteObjectRequest
			{
				BucketName = BucketName,
				Key = key
			};

			try
			{
				var response = S3Client.DeleteObjectAsync ( request ).GetAwaiter ().GetResult ();
			}
			catch ( Exception )
			{
			}

			try
			{
				CacheDirectory.DeleteFile ( name );
			}
			catch ( Exception )
			{
			}
		}

		/// <summary>Returns the length of a file in the directory. </summary>
		public override long FileLength ( string name )
		{
			var request = new GetObjectMetadataRequest
			{
				BucketName = BucketName,
				Key = GetBlobName ( name )
			};

			try
			{
				var response = S3Client.GetObjectMetadataAsync ( request ).GetAwaiter ().GetResult ();
				return response.ContentLength;
			}
			catch
			{
				return 0;
			}
		}

		public bool DownloadBlob ( string name, Stream outStream )
		{
			var request = new GetObjectRequest
			{
				BucketName = BucketName,
				Key = GetBlobName ( name )
			};

			try
			{
				var response = S3Client.GetObjectAsync ( request ).GetAwaiter ().GetResult ();
				using ( var responseStream = response.ResponseStream )
				{
					responseStream.CopyTo ( outStream );
					return ( response.HttpStatusCode == HttpStatusCode.OK );
				}
			}
			catch
			{
			}
			return false;
		}

		public bool UploadBlob ( string name, Stream inStream )
		{
			var request = new PutObjectRequest
			{
				BucketName = BucketName,
				Key = GetBlobName ( name ),
				InputStream = inStream
			};

			try
			{
				var response = S3Client.PutObjectAsync ( request ).GetAwaiter ().GetResult ();
				return ( response.HttpStatusCode == HttpStatusCode.OK );
			}
			catch
			{
			}
			return false;
		}

		public override void Sync ( ICollection<string> names )
		{
			// TODO: This all is purely guesswork, no idea what has to be done here. -- Aviad.
			foreach ( var name in names )
			{
				if ( _nameCache.ContainsKey ( name ) )
				{
					_nameCache[name].Flush ();
				}
			}
		}

		public override IndexInput OpenInput ( string name, IOContext context )
		{
			// TODO: Figure out how IOContext comes into play here. So far it doesn't -- Aviad
			try
			{
				var blobName = GetBlobName ( name );
				return new AwsS3IndexInput ( this, name );
			}
			catch ( Exception err )
			{
				throw new FileNotFoundException ( name, err );
			}
		}

		/// <summary>Construct a {@link Lock}.</summary>
		/// <param name="name">the name of the lock file
		/// </param>
		public override Lock MakeLock ( string name )
		{
			lock ( _locks )
			{
				if ( !_locks.ContainsKey ( name ) )
				{
					_locks.Add ( name, new AwsS3Lock ( name, this ) );
				}
				return _locks[name];
			}
		}

		public override void ClearLock ( string name )
		{
			lock ( _locks )
			{
				if ( _locks.ContainsKey ( name ) )
				{
					_locks[name].BreakLock ();
				}
			}
		}

		/// <summary>Closes the store. </summary>
		protected override void Dispose ( bool disposing )
		{
			S3Client = VcSoft.Utility.VcUtil.CleanMe ( S3Client );
		}

		public override void SetLockFactory ( LockFactory lockFactory )
		{
			_lockFactory = lockFactory;
		}

		/// <summary>Creates a new, empty file in the directory with the given name.
		/// Returns a stream writing this file. 
		/// </summary>
		public override IndexOutput CreateOutput ( string name, IOContext context )
		{
			// TODO: Figure out how IOContext comes into play here. So far it doesn't -- Aviad
			var indexOutput = new AwsS3IndexOutput ( this, name );
			_nameCache[name] = indexOutput;
			return indexOutput;
		}
		#endregion

		#region internal methods

		public string GetBlobName ( string name )
		{
			return _subDirectory.Length > 1 ? $"{_subDirectory}/{name}" : name;
		}

		private void InitCacheDirectory ( Directory cacheDirectory )
		{
			if ( cacheDirectory != null )
			{
				// save it off
				CacheDirectory = cacheDirectory;
				return;
			}

			string cachePath;
			if (! string.IsNullOrEmpty ( Environment.GetEnvironmentVariable ( "LAMBDA_TASK_ROOT" ) ) )
			{
				cachePath = "/tmp";
			}
			else
			{
				cachePath = Environment.ExpandEnvironmentVariables ( "%temp%" );
			}

			cachePath = Path.Combine ( cachePath, "AwsS3Directory" );

			DirectoryInfo localDir = new DirectoryInfo ( cachePath );
			if ( !localDir.Exists )
				localDir.Create ();

			string catalogPath = Path.Combine ( cachePath, Name );

			DirectoryInfo catalogDir = new DirectoryInfo ( catalogPath );
			if ( !catalogDir.Exists )
				catalogDir.Create ();

			CacheDirectory = FSDirectory.Open ( catalogPath );
		}

		public StreamInput OpenCachedInputAsStream ( string name )
		{
			return new StreamInput ( CacheDirectory.OpenInput ( name, IOContext.DEFAULT ) );
		}

		public StreamOutput CreateCachedOutputAsStream ( string name )
		{
			return new StreamOutput ( CacheDirectory.CreateOutput ( name, IOContext.DEFAULT ) );
		}
		#endregion
	}
}
