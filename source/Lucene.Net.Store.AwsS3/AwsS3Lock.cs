//    License: Microsoft Public License (Ms-PL) 
using System;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Amazon.S3.Model;

namespace Lucene.Net.Store.AwsS3
{
	/// <summary>
	/// Implements lock semantics on AwsS3Directory via a object lease
	/// </summary>
	public class AwsS3Lock : Lock
	{
		private const int mciLockDurationSeconds = 60;
		private const int mciLeaseIDLength = 35;
		private const char mccLockSeparator = '%';

		private string _lockFile;
		private readonly string _bucketName;
		private AwsS3Directory _awsDirectory;
		private string _leaseid;

		public AwsS3Lock ( string lockFile, AwsS3Directory directory )
		{
			_lockFile = directory.GetBlobName ( lockFile );

			_bucketName = directory.BucketName;
			_awsDirectory = directory;
		}

		#region S3 Locking

		public class LockInfo
		{
			public string LeaseID { get; set; }
			public DateTime TimeExpiryDate { get; set; } = DateTime.MinValue;

			public bool IsValid { get { return !string.IsNullOrEmpty ( LeaseID ) && TimeExpiryDate > DateTime.MinValue; } }
		}

		private string GetRandomBody ( int length )
		{
			var rand = new Random ();
			var bfr = new StringBuilder ();
			var lineLength = 0;

			while ( bfr.Length < length )
			{
				if ( lineLength > 0 && lineLength % 20 == 0 )
					if ( lineLength < 50 )
						bfr.Append ( '-' );
					else
						bfr.AppendLine ();
				var ch = rand.Next ( 65 );
				ch = ch < 10 ? ( '0' + ch ) : ( ch < 36 ? 'A' + ch - 10 : 'a' + ch - 36 );
				bfr.Append ( (char)ch );
			}
			return bfr.ToString ();
		}

		private async Task<LockInfo> ReadLockFileAsync ( string objectName )
		{
			var request = new GetObjectRequest
			{
				BucketName = _bucketName,
				Key = objectName,
			};

			try
			{
				var response = await _awsDirectory.Client.GetObjectAsync ( request );

				if ( response.HttpStatusCode != HttpStatusCode.OK )
					return null;

				using ( var responseStream = response.ResponseStream )
				{
					using ( var ms = new MemoryStream () )
					{
						responseStream.CopyTo ( ms );
						ms.Seek ( 0, SeekOrigin.Begin );

						StreamReader reader = new StreamReader ( ms );
						var content = reader.ReadToEnd ();

						var idx = content.IndexOf ( mccLockSeparator );
						if ( idx < 5 )
							return new LockInfo ();

						long ts;
						if ( !long.TryParse ( content.Substring ( 0, idx ), out ts ) )
							return new LockInfo ();

						return new LockInfo { LeaseID = content.Substring ( idx + 1 ), TimeExpiryDate = new DateTime ( ts, DateTimeKind.Utc ) };
					}
				}
			}
			//catch ( AmazonS3Exception ex )
			//{
			//    return null;
			//}
			catch ( Exception ex )
			{
				//Console.WriteLine ( ex );
			}
			return null;
		}

		private Task<LockInfo> WriteNewLockFileAsync ( string objectName, TimeSpan expiration )
		{
			var id = GetRandomBody ( mciLeaseIDLength );
			return WriteLockFileAsync ( objectName, id, expiration );
		}

		private async Task<LockInfo> WriteLockFileAsync ( string objectName, string leaseID, TimeSpan expiration )
		{
			var expDate = DateTime.UtcNow + expiration;
			var content = new StringBuilder ();
			content.Append ( expDate.Ticks );
			content.Append ( mccLockSeparator );
			content.Append ( leaseID );
			var request = new PutObjectRequest
			{
				BucketName = _bucketName,
				Key = objectName,
				ContentBody = content.ToString ()
			};

			try
			{
				var result = await _awsDirectory.Client.PutObjectAsync ( request );

				if ( result.HttpStatusCode != HttpStatusCode.OK )
					return null;
			}
			catch ( Exception ex )
			{
				Console.WriteLine ( ex );
				return null;
			}

			// Read it back...

			var info = await ReadLockFileAsync ( objectName );
			if ( info is null || !info.IsValid )
			{
				// Can not read it back
				return null;
			}

			if ( expDate != info.TimeExpiryDate || leaseID != info.LeaseID )
				return null;

			return info;
		}

		private async Task<bool> IsLockedAsync ( string objectName, string leaseID )
		{
			var lockInfo = await ReadLockFileAsync ( objectName );
			if ( lockInfo is null )
				return false;

			// It's locked.
			// First check if the lock is expired.

			if ( !lockInfo.IsValid || DateTime.UtcNow > lockInfo.TimeExpiryDate )
			{
				// Delete object
				_ = await DeleteFileAsync ( objectName );

				return false;
			}

			// Check if locked by us.

			return lockInfo.LeaseID != leaseID;
		}

		private async Task<bool> DeleteFileAsync ( string objectName )
		{
			var request = new DeleteObjectRequest
			{
				BucketName = _bucketName,
				Key = objectName,
			};

			try
			{
				var response = await _awsDirectory.Client.DeleteObjectAsync ( request );
				return ( response.HttpStatusCode == HttpStatusCode.NoContent );
			}
			catch ( Exception ex )
			{
				//Console.WriteLine ( ex );
			}
			return false;
		}

		private async Task<bool> ReleaseAsync ( string objectName, string leaseID )
		{
			var lockInfo = await ReadLockFileAsync ( objectName );
			if ( lockInfo is null )
				return true;

			if ( lockInfo.IsValid )
			{
				// Check if this is locked by somebody else...
				if ( lockInfo.LeaseID != leaseID && DateTime.UtcNow < lockInfo.TimeExpiryDate )
				{
					return false;
				}
			}

			return await DeleteFileAsync ( objectName );
		}

		#endregion

		#region Lock methods
		override public bool IsLocked ()
		{
			Debug.WriteLine ( $"{_awsDirectory.Name} IsLocked() : {_leaseid}" );

			var result = IsLockedAsync ( _lockFile, _leaseid ).GetAwaiter ().GetResult ();
			Debug.Print ( $"IsLocked() : {result}" );

			return result;
		}

		public override bool Obtain ()
		{
			if ( string.IsNullOrEmpty ( _leaseid ) )
			{
				var lease = WriteNewLockFileAsync ( _lockFile, TimeSpan.FromSeconds ( mciLockDurationSeconds ) )
					.GetAwaiter ().GetResult ();

				if ( lease != null && lease.IsValid )
				{
					_leaseid = lease.LeaseID;

					// keep the lease alive by renewing every 30 seconds
					long interval = (long)TimeSpan.FromSeconds ( mciLockDurationSeconds / 2 ).TotalMilliseconds;
					_renewTimer = new Timer ( ( obj ) =>
					{
						try
						{
							var al = (AwsS3Lock)obj;
							al.Renew ();
						}
						catch ( Exception err ) { Debug.Print ( err.ToString () ); }
					}, this, interval, interval );
				}
			}
			return !string.IsNullOrEmpty ( _leaseid );
		}

		private Timer _renewTimer;

		public void Renew ()
		{
			if ( !string.IsNullOrEmpty ( _leaseid ) )
			{
				Debug.Print ( "AwsS3Lock:Renew({0} : {1}", _lockFile, _leaseid );

				_ = WriteLockFileAsync ( _lockFile, _leaseid, TimeSpan.FromSeconds ( mciLockDurationSeconds ) )
				   .GetAwaiter ().GetResult ();
			}
		}

		#endregion

		public void BreakLock ()
		{
			Debug.Print ( "AwsS3Lock:BreakLock({0}) {1}", _lockFile, _leaseid );
			if ( !string.IsNullOrEmpty ( _leaseid ) )
			{
				_ = ReleaseAsync ( _lockFile, _leaseid );
			}
			_leaseid = null;
		}

		public override System.String ToString ()
		{
			return $"{_awsDirectory.Name} AwsS3Lock@{_lockFile}.{_leaseid}";
		}

		protected override void Dispose ( Boolean disposing )
		{
			Debug.WriteLine ( $"{_awsDirectory.Name} AwsS3Lock:Release({_lockFile}) {_leaseid}" );
			if ( !string.IsNullOrEmpty ( _leaseid ) )
			{
				_ = ReleaseAsync ( _lockFile, _leaseid )
					.GetAwaiter ().GetResult ();

				if ( _renewTimer != null )
				{
					_renewTimer.Dispose ();
					_renewTimer = null;
				}
				_leaseid = null;
			}
		}
	}
}

