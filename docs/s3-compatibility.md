S3 Compatibility
================

## Compatibility Table

| Name                                        | Support | Planned support                                              | Caveats                                             |
|:-------------------------------------------:|:-------:|:------------------------------------------------------------:|:---------------------------------------------------:|
| AbortMultipartUpload                        | Full    |                                                              |                                                     |
| CompleteMultipartUpload                     | Full    |                                                              |                                                     |
| CopyObject                                  | Full    |                                                              |                                                     |
| CreateBucket                                | Full    |                                                              |                                                     |
| CreateMultipartUpload                       | Full    |                                                              |                                                     |
| DeleteBucket                                | Full    |                                                              |                                                     |
| DeleteBucketAnalyticsConfiguration          | No      | No                                                           |                                                     |
| DeleteBucketCors                            | No      | No                                                           |                                                     |
| DeleteBucketEncryption                      | No      | No                                                           |                                                     |
| DeleteBucketIntelligentTieringConfiguration | No      | No                                                           |                                                     |
| DeleteBucketInventoryConfiguration          | No      | No                                                           |                                                     |
| DeleteBucketLifecycle                       | No      | No                                                           |                                                     |
| DeleteBucketMetricsConfiguration            | No      | No                                                           |                                                     |
| DeleteBucketOwnershipControls               | No      | No                                                           |                                                     |
| DeleteBucketPolicy                          | No      | No                                                           |                                                     |
| DeleteBucketReplication                     | No      | No                                                           |                                                     |
| DeleteBucketTagging                         | No      | We could support this                                        |                                                     |
| DeleteBucketWebsite                         | No      | No                                                           |                                                     |
| DeleteObject                                | Full    |                                                              |                                                     |
| DeleteObjectTagging                         | Full    |                                                              | Tags can be modified outside of tagging endpoints   |
| DeleteObjects                               | Full    |                                                              |                                                     |
| DeletePublicAccessBlock                     | No      | No                                                           |                                                     |
| GetBucketAccelerateConfiguration            | No      | No                                                           |                                                     |
| GetBucketAcl                                | No      | No                                                           |                                                     |
| GetBucketAnalyticsConfiguration             | No      | No                                                           |                                                     |
| GetBucketCors                               | No      | No                                                           |                                                     |
| GetBucketEncryption                         | No      | No                                                           |                                                     |
| GetBucketIntelligentTieringConfiguration    | No      | No                                                           |                                                     |
| GetBucketInventoryConfiguration             | No      | No                                                           |                                                     |
| GetBucketLifecycle (deprecated)             | No      | We could partially support this                              |                                                     |
| GetBucketLifecycleConfiguration             | No      | We could partially support this                              |                                                     |
| GetBucketLocation                           | No      | We could support this                                        | Location constraints would be different from AWS S3 |
| GetBucketLogging                            | No      | No                                                           |                                                     |
| GetBucketMetricsConfiguration               | No      | No                                                           |                                                     |
| GetBucketNotification (deprecated)          | No      | No                                                           |                                                     |
| GetBucketNotificationConfiguration          | No      | No                                                           |                                                     |
| GetBucketOwnershipControls                  | No      | No                                                           |                                                     |
| GetBucketPolicy                             | Partial |                                                              | Only in Gateway-ST with --website                   |
| GetBucketPolicyStatus                       | No      | We could support this                                        | Currently, it always returns false                  |
| GetBucketReplication                        | No      | No                                                           |                                                     |
| GetBucketRequestPayment                     | No      | No                                                           | Planned support status needs verification           |
| GetBucketTagging                            | No      | We could support this                                        |                                                     |
| GetBucketVersioning                         | No      | Planned. See https://github.com/storj/roadmap/issues/23      |                                                     |
| GetBucketWebsite                            | No      | No                                                           |                                                     |
| GetObject                                   | Partial |                                                              | We need to add support for the partNumber parameter |
| GetObjectAcl                                | No      | No                                                           |                                                     |
| GetObjectLegalHold                          | No      | No                                                           |                                                     |
| GetObjectLockConfiguration                  | No      | No                                                           |                                                     |
| GetObjectRetention                          | No      | No                                                           |                                                     |
| GetObjectTagging                            | Full    |                                                              | Tags can be modified outside of tagging endpoints   |
| GetObjectTorrent                            | No      | With significant effort, we could support this               |                                                     |
| GetPublicAccessBlock                        | No      | No                                                           |                                                     |
| HeadBucket                                  | Full    |                                                              |                                                     |
| HeadObject                                  | Full    |                                                              |                                                     |
| ListBucketAnalyticsConfigurations           | No      | No                                                           |                                                     |
| ListBucketIntelligentTieringConfigurations  | No      | No                                                           |                                                     |
| ListBucketInventoryConfigurations           | No      | No                                                           |                                                     |
| ListBucketMetricsConfigurations             | No      | No                                                           |                                                     |
| ListBuckets                                 | Full    |                                                              |                                                     |
| ListMultipartUploads                        | Partial | Planned full. See https://github.com/storj/roadmap/issues/20 | TODO(artur): support status needs verification      |
| ListObjectVersions                          | No      | Planned. See https://github.com/storj/roadmap/issues/23      |                                                     |
| ListObjects                                 | Partial | Planned full. See https://github.com/storj/roadmap/issues/20 | See ListObjects section                             |
| ListObjectsV2                               | Partial | Planned full. See https://github.com/storj/roadmap/issues/20 | See ListObjects section                             |
| ListParts                                   | Full    |                                                              |                                                     |
| PutBucketAccelerateConfiguration            | No      | No                                                           |                                                     |
| PutBucketAcl                                | No      | No                                                           |                                                     |
| PutBucketAnalyticsConfiguration             | No      | No                                                           |                                                     |
| PutBucketCors                               | No      | No                                                           |                                                     |
| PutBucketEncryption                         | No      | No                                                           |                                                     |
| PutBucketIntelligentTieringConfiguration    | No      | No                                                           |                                                     |
| PutBucketInventoryConfiguration             | No      | No                                                           |                                                     |
| PutBucketLifecycle (deprecated)             | No      | No                                                           |                                                     |
| PutBucketLifecycleConfiguration             | No      | No                                                           |                                                     |
| PutBucketLogging                            | No      | No                                                           |                                                     |
| PutBucketMetricsConfiguration               | No      | No                                                           |                                                     |
| PutBucketNotification (deprecated)          | No      | No                                                           |                                                     |
| PutBucketNotificationConfiguration          | No      | No                                                           |                                                     |
| PutBucketOwnershipControls                  | No      | No                                                           |                                                     |
| PutBucketPolicy                             | No      | No                                                           |                                                     |
| PutBucketReplication                        | No      | No                                                           |                                                     |
| PutBucketRequestPayment                     | No      | No                                                           | Planned support status needs verification           |
| PutBucketTagging                            | No      | We could support this                                        |                                                     |
| PutBucketVersioning                         | No      | Planned. See https://github.com/storj/roadmap/issues/23      |                                                     |
| PutBucketWebsite                            | No      | No                                                           |                                                     |
| PutObject                                   | Full    |                                                              |                                                     |
| PutObjectAcl                                | No      | No                                                           |                                                     |
| PutObjectLegalHold                          | No      | No                                                           |                                                     |
| PutObjectLockConfiguration                  | No      | No                                                           |                                                     |
| PutObjectRetention                          | No      | No                                                           |                                                     |
| PutObjectTagging                            | Full    |                                                              | Tags can be modified outside of tagging endpoints   |
| PutPublicAccessBlock                        | No      | No                                                           |                                                     |
| RestoreObject                               | No      | No                                                           |                                                     |
| SelectObjectContent                         | No      | No                                                           |                                                     |
| UploadPart                                  | Full    |                                                              |                                                     |
| UploadPartCopy                              | No      | No                                                           | We may support it depending on reported needs       |
| WriteGetObjectResponse                      | No      | No                                                           |                                                     |

## Compatibility Table Support/Caveats

### Definitions

#### Full compatibility

Full compatibility means that we support all features of a specific action
except for features that rely on other actions that we haven't fully
implemented.

#### Partial compatibility

Partial compatibility means that we don't support all features of a specific
action (see Caveats column).

### ListObjects

A bucket's paths are end-to-end encrypted. We don't use an ordering-preserving
encryption scheme yet, meaning that it's impossible to always list a bucket in
lexicographical order (as per S3 specification). For requests that come with
forward-slash-terminated prefix and/or forward-slash delimiter, we list in the
fastest way we can, which will list a bucket in lexicographical order, but for
encrypted paths (which is often very different from the expected order for
decrypted paths). Ideally, clients shouldn't care about ordering in those cases.
For requests that come with non-forward-slash-terminated prefix and/or
non-forward-slash delimiter, we perform exhaustive listing, which will filter
paths gateway-side. In this case, gateways return listing in lexicographical
order. Forcing exhaustive listing for any request is not possible for Storj
production deployments of Gateway-MT, and for, e.g. Gateway-ST can be achieved
with `--s3.fully-compatible-listing`.

### ACL-related actions

[Secure access control in the decentralized
cloud](https://www.storj.io/blog/secure-access-control-in-the-decentralized-cloud)
is a good read for why we don't support ACL-related actions.

## Limits

|                                      Limit                                      |        Specification        |
|:-------------------------------------------------------------------------------:|:---------------------------:|
| Maximum number of buckets                                                       |             100             |
| Maximum number of objects per bucket                                            |           No limit          |
| Maximum object size                                                             |           No limit          |
| Minimum object size                                                             |             0 B             |
| Maximum object size per PUT operation                                           |           No limit          |
| Maximum number of parts per upload                                              |            10000            |
| Minimum part size                                                               | 5 MiB. Last part can be 0 B |
| Maximum number of parts returned per list parts request                         |            10000            |
| Maximum number of objects returned per list objects request                     |             1000            |
| Maximum number of multipart uploads returned per list multipart uploads request |             1000            |
| Maximum length for bucket names                                                 |              63             |
| Minimum length for bucket names                                                 |              3              |
| Maximum length for encrypted object names                                       |             1280            |
| Maximum metadata size                                                           |            2 KiB            |
