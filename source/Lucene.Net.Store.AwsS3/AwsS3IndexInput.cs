//    License: Microsoft Public License (Ms-PL) 
using System;
using System.Diagnostics;
using System.Threading;


namespace Lucene.Net.Store.AwsS3
{
	/// <summary>
	/// Implements IndexInput semantics for a read only blob
	/// </summary>
	public class AwsS3IndexInput : IndexInput
	{
		private readonly string _name;
		private AwsS3Directory _awsDirectory;
		private IndexInput _indexInput;
		private Mutex _fileMutex;

		public AwsS3IndexInput ( AwsS3Directory awsDirectory, string name )
			: base ( name )
		{
			_name = name;
			_awsDirectory = awsDirectory;
#if FULLDEBUG
			Debug.WriteLine ( $"{_awsDirectory.Name} opening {name} " );
#endif
			_fileMutex = BlobMutexManager.GrabMutex ( name );
			_fileMutex.WaitOne ();
			try
			{
				bool fileNeeded = false;
				if ( !CacheDirectory.FileExists ( name ) )
				{
					fileNeeded = true;
				}
				else
				{
					long cachedLength = CacheDirectory.FileLength ( name );
					long blobLength = awsDirectory.FileLength ( name );
					if ( cachedLength != blobLength )
						fileNeeded = true;
				}

				// if the file does not exist
				// or if it exists and it is older then the lastmodified time in the blobproperties (which always comes from the blob storage)
				if ( fileNeeded )
				{
					using ( StreamOutput fileStream = _awsDirectory.CreateCachedOutputAsStream ( name ) )
					{
						// get the blob
						_awsDirectory.DownloadBlob ( name, fileStream );
						fileStream.Flush ();

						Debug.WriteLine ( $"{_awsDirectory.Name} GET {_name} RETREIVED {fileStream.Length} bytes" );
					}
				}
#if FULLDEBUG
				Debug.WriteLine ( $"{_awsDirectory.Name} Using cached file for {name}" );
#endif
				// and open it as our input, this is now available forevers until new file comes along
				_indexInput = CacheDirectory.OpenInput ( name, IOContext.DEFAULT );

			}
			finally
			{
				_fileMutex.ReleaseMutex ();
			}
		}

		public Lucene.Net.Store.Directory CacheDirectory { get { return _awsDirectory.CacheDirectory; } }

		public override byte ReadByte ()
		{
			return _indexInput.ReadByte ();
		}

		public override void ReadBytes ( byte[] b, int offset, int len )
		{
			_indexInput.ReadBytes ( b, offset, len );
		}

		public override void Seek ( long pos )
		{
			_indexInput?.Seek ( pos );
		}

		public override long Length => _indexInput.Length;

		public override long Position => _indexInput.Position;

		protected override void Dispose ( bool disposing )
		{
			_fileMutex.WaitOne ();
			try
			{
#if FULLDEBUG
				Debug.WriteLine ( $"{_awsDirectory.Name} CLOSED READSTREAM local {_name}" );
#endif
				_indexInput.Dispose ();
				_indexInput = null;
				_awsDirectory = null;
				GC.SuppressFinalize ( this );
			}
			finally
			{
				_fileMutex.ReleaseMutex ();
			}
		}

		public override Object Clone ()
		{
			var clone = new AwsS3IndexInput ( this._awsDirectory, this._name );
			clone.Seek ( this.Position );
			return clone;
		}
	}
}