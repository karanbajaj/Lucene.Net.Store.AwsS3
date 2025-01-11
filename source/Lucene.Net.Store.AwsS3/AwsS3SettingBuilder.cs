//    License: Microsoft Public License (Ms-PL) 
using System;
using System.Linq;
using Amazon;
using Amazon.Runtime;
using Amazon.S3;

namespace Lucene.Net.Store.AwsS3
{
    public class AwsS3SettingBuilder
    {
        private readonly AwsS3Settings _settings = new AwsS3Settings();

        public AwsS3SettingBuilder ConnectionString(string connectionString)
        {
            if (String.IsNullOrEmpty(connectionString))
                throw new ArgumentNullException(nameof(connectionString));
            _settings.ConnectionString = connectionString;
            return this;
        }
        public AwsS3SettingBuilder BucketName(string bucketName)
        {
            _settings.BucketName = bucketName;
            return this;
        }
		public AwsS3SettingBuilder AccessKey(string accessKey)
        {
            _settings.AccessKey = accessKey;
            return this;
        }
        public AwsS3SettingBuilder SecretKey(string secretKey)
        {
            _settings.SecretKey = secretKey;
            return this;
        }

        public AwsS3SettingBuilder BucketFolder(string bucketFolder)
        {
            _settings.BucketFolder = bucketFolder;
            return this;
        }

        public AwsS3SettingBuilder Credentials(AWSCredentials credentials)
        {
            _settings.Credentials = credentials;
            return this;
        }
        public AwsS3SettingBuilder Credentials(string accessKey, string secretKey)
        {
            if (String.IsNullOrEmpty(accessKey))
                throw new ArgumentNullException(nameof(accessKey));
            if (String.IsNullOrEmpty(secretKey))
                throw new ArgumentNullException(nameof(secretKey));

            _settings.Credentials = new BasicAWSCredentials(accessKey, secretKey);
            return this;
        }

        public AwsS3SettingBuilder Region(RegionEndpoint region)
        {
            _settings.Region = region.ToString();
            return this;
        }

        public AwsS3SettingBuilder ServiceUrl(string serviceUrl)
        {
            _settings.ServiceUrl = serviceUrl;
            return this;
        }

        public AwsS3SettingBuilder UseChunkEncoding(bool useChunkEncoding)
        {
            _settings.UseChunkEncoding = useChunkEncoding;
            return this;
        }

        public AwsS3SettingBuilder CannedACL(S3CannedACL cannedACL)
        {
            _settings.CannedACL = cannedACL;
            return this;
        }
        public AwsS3SettingBuilder CannedACL(string cannedAcl)
        {
            if (String.IsNullOrEmpty(cannedAcl))
                throw new ArgumentNullException(nameof(cannedAcl));
            _settings.CannedACL = S3CannedACL.FindValue(cannedAcl);
            return this;
        }

        public AwsS3Settings Build()
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
}
