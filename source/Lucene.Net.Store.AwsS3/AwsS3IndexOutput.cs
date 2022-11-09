using System;
using System.Diagnostics;
using System.Threading;

namespace Lucene.Net.Store.AwsS3
{
	/// <summary>
	/// Implements IndexOutput semantics for a write/append straight to blob storage
	/// </summary>
	public class AwsS3IndexOutput : IndexOutput
	{
		private AwsS3Directory _awsDirectory;
		private readonly string _name;
		private IndexOutput _indexOutput;
		private Mutex _fileMutex;

		public AwsS3IndexOutput ( AwsS3Directory directory, string name )
		{
			_name = name;
			_fileMutex = BlobMutexManager.GrabMutex ( _name );
			_fileMutex.WaitOne ();
			try
			{
				_awsDirectory = directory;

				// create the local cache one we will operate against...
				_indexOutput = CacheDirectory.CreateOutput ( _name, IOContext.DEFAULT );
			}
			finally
			{
				_fileMutex.ReleaseMutex ();
			}
		}

		public Lucene.Net.Store.Directory CacheDirectory { get { return _awsDirectory.CacheDirectory; } }

		public override void Flush ()
		{
			_indexOutput?.Flush ();
		}

		protected override void Dispose ( bool disposing )
		{
			_fileMutex.WaitOne ();
			try
			{
				// make sure it's all written out
				_indexOutput.Flush ();

				long originalLength = _indexOutput.Length;
				_indexOutput.Dispose ();

				using ( var blobStream = new StreamInput ( CacheDirectory.OpenInput ( _name, IOContext.DEFAULT ) ) )
				{
					// push the blobStream up to the cloud
					_awsDirectory.UploadBlob ( _name, blobStream );

					// set the metadata with the original index file properties
					//_blob.SetMetadata();

					Debug.WriteLine ( $"{_awsDirectory.Name} PUT {_name} bytes to {blobStream.Length} in cloud" );
				}

#if FULLDEBUG
				Debug.WriteLine ( $"{_awsDirectory.Name} CLOSED WRITESTREAM {_name}" );
#endif
				// clean up
				_indexOutput = null;
				GC.SuppressFinalize ( this );
			}
			finally
			{
				_fileMutex.ReleaseMutex ();
			}
		}

		public override long Length => _indexOutput.Length;

		public override void WriteByte ( byte b )
		{
			_indexOutput.WriteByte ( b );
		}

		public override void WriteBytes ( byte[] b, int length )
		{
			_indexOutput.WriteBytes ( b, length );
		}

		public override void WriteBytes ( byte[] b, int offset, int length )
		{
			_indexOutput.WriteBytes ( b, offset, length );
		}

		[Obsolete ( "(4.1) this method will be removed in Lucene 5.0" )]
		public override void Seek ( long pos )
		{
			// obsolete
		}

		public override long Checksum => _indexOutput.Checksum;

		public override long Position => _indexOutput.Position;
	}
}
