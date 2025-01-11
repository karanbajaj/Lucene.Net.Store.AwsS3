//    License: Microsoft Public License (Ms-PL) 
using System;
using Amazon;
using Amazon.Runtime;
using Amazon.S3;

namespace Lucene.Net.Store.AwsS3
{
    public class AwsS3Settings
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
}
