// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package miniogw_test

import (
	"bytes"
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"storj.io/common/grant"
	"storj.io/common/macaroon"
	"storj.io/common/memory"
	"storj.io/common/storj"
	"storj.io/common/testcontext"
	"storj.io/common/testrand"
	"storj.io/storj/private/testplanet"
	"storj.io/storj/satellite"
)

const (
	lockModeCompliance = s3.ObjectLockModeCompliance
	lockModeGovernance = s3.ObjectLockModeGovernance
	legalHoldOn        = s3.ObjectLockLegalHoldStatusOn
	legalHoldOff       = s3.ObjectLockLegalHoldStatusOff
)

func TestObjectLockRestrictedPermissions(t *testing.T) {
	runIntegrationTest(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 0,
		UplinkCount:      1,
		Reconfigure: testplanet.Reconfigure{
			Uplink: func(log *zap.Logger, index int, config *testplanet.UplinkConfig) {
				config.APIKeyVersion = macaroon.APIKeyVersionObjectLock
			},
		},
	}, func(ctx *testcontext.Context, t *testing.T, planet *testplanet.Planet, gw *testGateway) {
		satellite := planet.Satellites[0]
		projectID := planet.Uplinks[0].Projects[0].ID
		ownerID := planet.Uplinks[0].Projects[0].Owner.ID
		apiKey := planet.Uplinks[0].APIKey[planet.Satellites[0].ID()]

		encAccess := grant.NewEncryptionAccessWithDefaultKey(&storj.Key{})
		encAccess.SetDefaultPathCipher(storj.EncNull)

		allowedClient := gw.clientFor(t, grantAccess(t, satellite.URL(), apiKey, encAccess))

		bucket := testrand.BucketName()
		require.NoError(t, createBucket(ctx, allowedClient, bucket, true, true))

		objKey1 := "testobject1"

		retainUntil := time.Now().Add(10 * time.Minute)

		t.Run("api key version disallows object lock", func(t *testing.T) {
			userCtx, err := satellite.UserContext(ctx, ownerID)
			require.NoError(t, err)

			_, apiKey, err := satellite.API.Console.Service.CreateAPIKey(userCtx, projectID, "restricted", macaroon.APIKeyVersionMin)
			require.NoError(t, err)

			client := gw.clientFor(t, grantAccess(t, satellite.URL(), apiKey, encAccess))

			requireS3Error(t, createBucket(ctx, client, testrand.BucketName(), true, true), http.StatusForbidden, "AccessDenied")
		})

		restrictedClient := func(t *testing.T, caveat macaroon.Caveat) *s3.S3 {
			restrictedApiKey, err := apiKey.Restrict(caveat)
			require.NoError(t, err)

			return gw.clientFor(t, grantAccess(t, satellite.URL(), restrictedApiKey, encAccess))
		}

		t.Run("disallow put retention", func(t *testing.T) {
			client := restrictedClient(t, macaroon.Caveat{
				DisallowPutRetention: true,
			})

			_, err := putObjectWithRetention(ctx, client, bucket, objKey1, lockModeCompliance, retainUntil)
			requireS3Error(t, err, http.StatusForbidden, "AccessDenied")

			putResp, err := putObjectWithRetention(ctx, allowedClient, bucket, objKey1, lockModeCompliance, retainUntil)
			require.NoError(t, err)

			_, err = putObjectRetention(ctx, client, bucket, objKey1, lockModeCompliance, retainUntil, *putResp.VersionId)
			requireS3Error(t, err, http.StatusForbidden, "AccessDenied")
		})

		t.Run("allow put retention implicitly allows get retention", func(t *testing.T) {
			client := restrictedClient(t, macaroon.Caveat{
				DisallowGetRetention: true,
			})

			putResp, err := putObjectWithRetention(ctx, client, bucket, objKey1, lockModeCompliance, retainUntil)
			require.NoError(t, err)

			retResp, err := getObjectRetention(ctx, client, bucket, objKey1, *putResp.VersionId)
			require.NoError(t, err)
			require.Equal(t, lockModeCompliance, *retResp.Retention.Mode)
			require.WithinDuration(t, retainUntil, *retResp.Retention.RetainUntilDate, time.Minute)
		})

		t.Run("disallow put legal hold", func(t *testing.T) {
			client := restrictedClient(t, macaroon.Caveat{
				DisallowPutLegalHold: true,
			})

			putResp, err := putObject(ctx, client, bucket, objKey1, nil)
			require.NoError(t, err)

			_, err = putObjectLegalHold(ctx, client, bucket, objKey1, legalHoldOn, *putResp.VersionId)
			requireS3Error(t, err, http.StatusForbidden, "AccessDenied")

			_, err = putObjectWithLegalHold(ctx, client, bucket, objKey1, legalHoldOn)
			requireS3Error(t, err, http.StatusForbidden, "AccessDenied")

			lhResp, err := getObjectLegalHold(ctx, client, bucket, objKey1, *putResp.VersionId)
			require.NoError(t, err)
			require.Equal(t, legalHoldOff, *lhResp.LegalHold.Status)
		})

		t.Run("disallow get legal hold", func(t *testing.T) {
			client := restrictedClient(t, macaroon.Caveat{
				DisallowGetLegalHold: true,
			})

			putResp, err := putObject(ctx, client, bucket, objKey1, nil)
			require.NoError(t, err)

			_, err = putObjectLegalHold(ctx, client, bucket, objKey1, legalHoldOn, *putResp.VersionId)
			require.NoError(t, err)

			_, err = putObjectWithLegalHold(ctx, client, bucket, objKey1, legalHoldOn)
			require.NoError(t, err)

			_, err = getObjectLegalHold(ctx, client, bucket, objKey1, *putResp.VersionId)
			requireS3Error(t, err, http.StatusForbidden, "AccessDenied")
		})

		t.Run("disallow governance bypass", func(t *testing.T) {
			client := restrictedClient(t, macaroon.Caveat{
				DisallowBypassGovernanceRetention: true,
			})

			putResp, err := putObjectWithRetention(ctx, client, bucket, objKey1, lockModeGovernance, retainUntil)
			require.NoError(t, err)

			requireS3Error(t, deleteObjectBypassGovernance(ctx, client, bucket, objKey1, *putResp.VersionId), http.StatusForbidden, "AccessDenied")
		})

		t.Run("disallow put object lock configuration", func(t *testing.T) {
			client := restrictedClient(t, macaroon.Caveat{
				DisallowPutBucketObjectLockConfiguration: true,
			})

			retDays := int64(5)
			retMode := s3.ObjectLockRetentionModeCompliance

			_, err := putObjectLockConfiguration(ctx, client, bucket, "Enabled", &s3.ObjectLockRule{
				DefaultRetention: &s3.DefaultRetention{
					Days: &retDays,
					Mode: &retMode,
				},
			})
			requireS3Error(t, err, http.StatusForbidden, "AccessDenied")
		})

		t.Run("disallow get object lock configuration", func(t *testing.T) {
			client := restrictedClient(t, macaroon.Caveat{
				DisallowGetBucketObjectLockConfiguration: true,
			})

			retDays := int64(5)
			retMode := s3.ObjectLockRetentionModeCompliance

			_, err := putObjectLockConfiguration(ctx, client, bucket, "Enabled", &s3.ObjectLockRule{
				DefaultRetention: &s3.DefaultRetention{
					Days: &retDays,
					Mode: &retMode,
				},
			})
			require.NoError(t, err)

			_, err = getObjectLockConfiguration(ctx, client, bucket)
			requireS3Error(t, err, http.StatusForbidden, "AccessDenied")
		})
	})
}

func TestObjectLock(t *testing.T) {
	runIntegrationTest(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 0,
		UplinkCount:      1,
		NonParallel:      true,
		Reconfigure: testplanet.Reconfigure{
			Satellite: func(log *zap.Logger, index int, config *satellite.Config) {
				config.Metainfo.ProjectLimits.MaxBuckets = 100
			},
			Uplink: func(log *zap.Logger, index int, config *testplanet.UplinkConfig) {
				config.APIKeyVersion = macaroon.APIKeyVersionObjectLock
			},
		},
	}, func(ctx *testcontext.Context, t *testing.T, planet *testplanet.Planet, gw *testGateway) {
		client := gw.client(t)

		bucket := testrand.BucketName()
		require.NoError(t, createBucket(ctx, client, bucket, true, true))

		objKey1, objKey2, objKey3 := "testobject1", "testobject2", "testobject3"

		retainUntil := time.Now().Add(10 * time.Minute)

		runRetentionModeTest := func(name string, f func(t *testing.T, mode string)) {
			t.Run(name+" (compliance mode)", func(t *testing.T) {
				f(t, lockModeCompliance)
			})
			t.Run(name+" (governance mode)", func(t *testing.T) {
				f(t, lockModeGovernance)
			})
		}

		t.Run("enable and disable object lock on bucket", func(t *testing.T) {
			noLockBucket := testrand.BucketName()
			require.NoError(t, createBucket(ctx, client, noLockBucket, true, false))

			_, err := getObjectLockConfiguration(ctx, client, noLockBucket)
			requireS3Error(t, err, http.StatusNotFound, "ObjectLockConfigurationNotFoundError")

			_, err = putObjectLockConfiguration(ctx, client, noLockBucket, "Disabled", nil)
			requireS3Error(t, err, http.StatusBadRequest, "MalformedXML")

			lockBucket := testrand.BucketName()
			require.NoError(t, createBucket(ctx, client, lockBucket, true, true))

			retDays := int64(5)
			retMode := s3.ObjectLockRetentionModeCompliance

			_, err = putObjectLockConfiguration(ctx, client, lockBucket, "Enabled", &s3.ObjectLockRule{
				DefaultRetention: &s3.DefaultRetention{
					Days: &retDays,
					Mode: &retMode,
				},
			})
			require.NoError(t, err)

			resp, err := getObjectLockConfiguration(ctx, client, lockBucket)
			require.NoError(t, err)
			require.Equal(t, "Enabled", *resp.ObjectLockConfiguration.ObjectLockEnabled)
			require.Equal(t, retMode, *resp.ObjectLockConfiguration.Rule.DefaultRetention.Mode)
			require.Equal(t, retDays, *resp.ObjectLockConfiguration.Rule.DefaultRetention.Days)
			require.Nil(t, resp.ObjectLockConfiguration.Rule.DefaultRetention.Years)

			retMode = s3.ObjectLockRetentionModeGovernance
			retYears := int64(1)

			_, err = putObjectLockConfiguration(ctx, client, lockBucket, "Enabled", &s3.ObjectLockRule{
				DefaultRetention: &s3.DefaultRetention{
					Years: &retYears,
					Mode:  &retMode,
				},
			})
			require.NoError(t, err)

			resp, err = getObjectLockConfiguration(ctx, client, lockBucket)
			require.NoError(t, err)
			require.Equal(t, "Enabled", *resp.ObjectLockConfiguration.ObjectLockEnabled)
			require.Equal(t, retMode, *resp.ObjectLockConfiguration.Rule.DefaultRetention.Mode)
			require.Equal(t, retYears, *resp.ObjectLockConfiguration.Rule.DefaultRetention.Years)
			require.Nil(t, resp.ObjectLockConfiguration.Rule.DefaultRetention.Days)

			putObjResp, err := putObject(ctx, client, lockBucket, objKey1, nil)
			require.NoError(t, err)

			objInfo, err := getObject(ctx, client, lockBucket, objKey1, *putObjResp.VersionId)
			require.NoError(t, err)

			require.Equal(t, retMode, *objInfo.ObjectLockMode)
			require.WithinDuration(t, time.Now().AddDate(int(retYears), 0, 0), *objInfo.ObjectLockRetainUntilDate, time.Minute)

			requireS3Error(t, deleteObject(ctx, client, lockBucket, objKey1, *putObjResp.VersionId), http.StatusForbidden, "AccessDenied")
		})

		t.Run("put object with default retention encompassing leap day", func(t *testing.T) {
			test := func(t *testing.T, defaultRetentionYears, defaultRetentionDays int, expectedRetainUntil time.Time) {
				bucketName := testrand.BucketName()
				require.NoError(t, createBucket(ctx, client, bucketName, true, true))

				defaultRetention := s3.DefaultRetention{
					Mode: new(s3.ObjectLockModeCompliance),
				}
				if defaultRetentionYears != 0 {
					defaultRetention.Years = new(int64(defaultRetentionYears))
				} else if defaultRetentionDays != 0 {
					defaultRetention.Days = new(int64(defaultRetentionDays))
				}

				_, err := client.PutObjectLockConfigurationWithContext(ctx, &s3.PutObjectLockConfigurationInput{
					Bucket: &bucketName,
					ObjectLockConfiguration: &s3.ObjectLockConfiguration{
						ObjectLockEnabled: new("Enabled"),
						Rule: &s3.ObjectLockRule{
							DefaultRetention: &defaultRetention,
						},
					},
				})
				require.NoError(t, err)

				objectKey := "file.txt"

				_, err = putObject(ctx, client, bucketName, objectKey, nil)
				require.NoError(t, err)

				resp, err := getObjectRetention(ctx, client, bucketName, objectKey, "")
				require.NoError(t, err)

				require.WithinDuration(t, expectedRetainUntil, *resp.Retention.RetainUntilDate, time.Minute)
			}

			// Find the nearest date N years after the current date that lies after a leap day.
			now := time.Now()
			leapYear := now.Year()
			var leapDay time.Time
			for {
				if (leapYear%4 == 0 && leapYear%100 != 0) || (leapYear%400 == 0) {
					leapDay = time.Date(leapYear, time.February, 29, 0, 0, 0, 0, time.UTC)
					if leapDay.After(now) {
						break
					}
				}
				leapYear++
			}
			years := leapYear - now.Year()
			if now.AddDate(years, 0, 0).Before(leapDay) {
				years++
			}

			t.Run("Default retention as days", func(t *testing.T) {
				// Expect 1 day to always be considered a 24-hour period, with no adjustments
				// made to accommodate the leap day.
				test(t, 0, 365*years, time.Now().AddDate(0, 0, 365*years))
			})

			t.Run("Default retention as years", func(t *testing.T) {
				// Expect the retention period duration to take the leap day into account.
				test(t, years, 0, time.Now().AddDate(0, 0, 365*years+1))
			})
		})

		t.Run("put object lock config on unversioned bucket not allowed", func(t *testing.T) {
			bucket := testrand.BucketName()
			require.NoError(t, createBucket(ctx, client, bucket, false, false))

			retDays := int64(5)
			retMode := s3.ObjectLockRetentionModeCompliance

			_, err := putObjectLockConfiguration(ctx, client, bucket, "Enabled", &s3.ObjectLockRule{
				DefaultRetention: &s3.DefaultRetention{
					Days: &retDays,
					Mode: &retMode,
				},
			})
			requireS3Error(t, err, http.StatusConflict, "InvalidBucketState")
		})

		t.Run("put object with lock not allowed on unversioned bucket", func(t *testing.T) {

			noVersioningBucket := testrand.BucketName()
			require.NoError(t, createBucket(ctx, client, noVersioningBucket, false, false))

			_, err := putObjectWithRetention(ctx, client, noVersioningBucket, objKey1, lockModeCompliance, retainUntil)
			requireS3Error(t, err, http.StatusBadRequest, "InvalidRequest")
		})

		t.Run("put object with lock enables versioning implicitly", func(t *testing.T) {
			noVersioningBucket := testrand.BucketName()
			require.NoError(t, createBucket(ctx, client, noVersioningBucket, false, true))

			resp, err := putObjectWithRetention(ctx, client, bucket, objKey1, lockModeCompliance, retainUntil)
			require.NoError(t, err)
			require.NotEmpty(t, resp.VersionId)
		})

		t.Run("put object with lock not allowed when bucket lock disabled", func(t *testing.T) {
			noLockBucket := testrand.BucketName()
			require.NoError(t, createBucket(ctx, client, noLockBucket, true, false))

			_, err := putObjectWithRetention(ctx, client, noLockBucket, objKey1, lockModeCompliance, retainUntil)
			requireS3Error(t, err, http.StatusBadRequest, "InvalidRequest")
		})

		t.Run("suspending versioning is not allowed when object lock enabled on bucket", func(t *testing.T) {
			_, err := client.PutBucketVersioning(&s3.PutBucketVersioningInput{
				Bucket: new(bucket),
				VersioningConfiguration: &s3.VersioningConfiguration{
					Status: new(s3.BucketVersioningStatusSuspended),
				},
			})
			requireS3Error(t, err, http.StatusConflict, "InvalidBucketState")
		})

		t.Run("get and put object retention error handling", func(t *testing.T) {
			noLockBucket := testrand.BucketName()
			require.NoError(t, createBucket(ctx, client, noLockBucket, true, false))

			_, err := putObject(ctx, client, noLockBucket, objKey1, nil)
			require.NoError(t, err)

			_, err = getObjectRetention(ctx, client, noLockBucket, objKey1, "")
			requireS3Error(t, err, http.StatusBadRequest, "InvalidRequest")
			// Note: S3 returns 400 InvalidRequest for GetObjectRetention when the bucket has no lock configuration.
			// If the bucket does have lock configuration it instead returns 404 NoSuchObjectLockConfiguration.

			_, err = putObject(ctx, client, bucket, objKey1, nil)
			require.NoError(t, err)

			_, err = getObjectRetention(ctx, client, bucket, objKey1, "")
			requireS3Error(t, err, http.StatusNotFound, "NoSuchObjectLockConfiguration")

			_, err = putObjectRetention(ctx, client, "nonexistent", objKey1, lockModeCompliance, retainUntil, "")
			requireS3Error(t, err, http.StatusNotFound, "NoSuchBucket")

			_, err = getObjectRetention(ctx, client, "nonexistent", objKey1, "")
			requireS3Error(t, err, http.StatusNotFound, "NoSuchBucket")

			_, err = putObjectRetention(ctx, client, bucket, "nonexistent", lockModeCompliance, retainUntil, "")
			requireS3Error(t, err, http.StatusNotFound, "NoSuchKey")

			_, err = getObjectRetention(ctx, client, bucket, "nonexistent", "")
			requireS3Error(t, err, http.StatusNotFound, "NoSuchKey")

			_, err = putObjectRetention(ctx, client, bucket, objKey1, "invalidmode", retainUntil, "")
			requireS3Error(t, err, http.StatusBadRequest, "MalformedXML")

			_, err = putObjectWithRetention(ctx, client, bucket, objKey1, "invalidmode", retainUntil)
			requireS3Error(t, err, http.StatusBadRequest, "InvalidArgument")

			_, err = putObjectMultipartWithRetention(ctx, client, bucket, objKey1, "invalidmode", retainUntil)
			requireS3Error(t, err, http.StatusBadRequest, "InvalidArgument")
		})

		t.Run("legal hold", func(t *testing.T) {
			putResp, err := putObjectWithLegalHold(ctx, client, bucket, objKey1, legalHoldOn)
			require.NoError(t, err)

			requireS3Error(t, deleteObject(ctx, client, bucket, objKey1, *putResp.VersionId), http.StatusForbidden, "AccessDenied")

			mpResp, err := putObjectMultipartWithLegalHold(ctx, client, bucket, objKey2, legalHoldOn)
			require.NoError(t, err)

			requireS3Error(t, deleteObject(ctx, client, bucket, objKey2, *mpResp.VersionId), http.StatusForbidden, "AccessDenied")

			for _, key := range []string{objKey1, objKey2} {
				lhResp, err := getObjectLegalHold(ctx, client, bucket, key, "")
				require.NoError(t, err)
				require.Equal(t, legalHoldOn, *lhResp.LegalHold.Status)
			}

			putResp, err = putObject(ctx, client, bucket, objKey3, nil)
			require.NoError(t, err)

			_, err = putObjectLegalHold(ctx, client, bucket, objKey3, legalHoldOn, *putResp.VersionId)
			require.NoError(t, err)

			lhResp, err := getObjectLegalHold(ctx, client, bucket, objKey3, *putResp.VersionId)
			require.NoError(t, err)
			require.Equal(t, legalHoldOn, *lhResp.LegalHold.Status)

			getObjResp, err := getObject(ctx, client, bucket, objKey3, *putResp.VersionId)
			require.NoError(t, err)
			require.Equal(t, legalHoldOn, *getObjResp.ObjectLockLegalHoldStatus)

			requireS3Error(t, deleteObject(ctx, client, bucket, objKey3, *putResp.VersionId), http.StatusForbidden, "AccessDenied")

			_, err = putObjectLegalHold(ctx, client, bucket, objKey3, legalHoldOff, *putResp.VersionId)
			require.NoError(t, err)

			lhResp, err = getObjectLegalHold(ctx, client, bucket, objKey3, *putResp.VersionId)
			require.NoError(t, err)
			require.Equal(t, legalHoldOff, *lhResp.LegalHold.Status)

			require.NoError(t, deleteObject(ctx, client, bucket, objKey3, *putResp.VersionId))
		})

		t.Run("get and put legal hold error handling", func(t *testing.T) {
			noLockBucket := testrand.BucketName()
			require.NoError(t, createBucket(ctx, client, noLockBucket, true, false))

			_, err := putObject(ctx, client, noLockBucket, objKey1, nil)
			require.NoError(t, err)

			_, err = getObjectLegalHold(ctx, client, noLockBucket, objKey1, "")
			requireS3Error(t, err, http.StatusBadRequest, "InvalidRequest")

			_, err = putObjectLegalHold(ctx, client, "nonexistent", objKey1, legalHoldOn, "")
			requireS3Error(t, err, http.StatusNotFound, "NoSuchBucket")

			_, err = getObjectLegalHold(ctx, client, "nonexistent", objKey1, "")
			requireS3Error(t, err, http.StatusNotFound, "NoSuchBucket")

			_, err = putObjectLegalHold(ctx, client, bucket, "nonexistent", legalHoldOn, "")
			requireS3Error(t, err, http.StatusNotFound, "NoSuchKey")

			_, err = getObjectLegalHold(ctx, client, bucket, "nonexistent", "")
			requireS3Error(t, err, http.StatusNotFound, "NoSuchKey")

			_, err = putObjectLegalHold(ctx, client, bucket, objKey1, "invalidstatus", "")
			requireS3Error(t, err, http.StatusBadRequest, "MalformedXML")

			_, err = putObjectLegalHold(ctx, client, bucket, objKey1, "", "")
			requireS3Error(t, err, http.StatusBadRequest, "MalformedXML")

			_, err = putObjectWithLegalHold(ctx, client, bucket, objKey1, "invalidstatus")
			requireS3Error(t, err, http.StatusBadRequest, "InvalidArgument")

			_, err = putObjectMultipartWithLegalHold(ctx, client, bucket, objKey1, "invalidstatus")
			requireS3Error(t, err, http.StatusBadRequest, "InvalidArgument")
		})

		runRetentionModeTest("legal hold and retention", func(t *testing.T, mode string) {
			putResp, err := putObjectWithLegalHoldAndRetention(ctx, client, bucket, objKey1, legalHoldOn, mode, retainUntil)
			require.NoError(t, err)

			lhResp, err := getObjectLegalHold(ctx, client, bucket, objKey1, *putResp.VersionId)
			require.NoError(t, err)
			require.Equal(t, legalHoldOn, *lhResp.LegalHold.Status)

			retResp, err := getObjectRetention(ctx, client, bucket, objKey1, *putResp.VersionId)
			require.NoError(t, err)
			require.Equal(t, mode, *retResp.Retention.Mode)
			require.WithinDuration(t, retainUntil, *retResp.Retention.RetainUntilDate, time.Minute)

			requireS3Error(t, deleteObject(ctx, client, bucket, objKey1, *putResp.VersionId), http.StatusForbidden, "AccessDenied")

			if mode == lockModeGovernance {
				requireS3Error(t, deleteObjectBypassGovernance(ctx, client, bucket, objKey1, *putResp.VersionId), http.StatusForbidden, "AccessDenied")
			}

			_, err = putObjectLegalHold(ctx, client, bucket, objKey1, legalHoldOff, *putResp.VersionId)
			require.NoError(t, err)

			requireS3Error(t, deleteObject(ctx, client, bucket, objKey1, *putResp.VersionId), http.StatusForbidden, "AccessDenied")

			if mode == lockModeGovernance {
				require.NoError(t, deleteObjectBypassGovernance(ctx, client, bucket, objKey1, *putResp.VersionId))
			}
		})

		t.Run("changing retention mode", func(t *testing.T) {
			putResp, err := putObjectWithRetention(ctx, client, bucket, objKey1, lockModeGovernance, retainUntil)
			require.NoError(t, err)

			_, err = putObjectRetention(ctx, client, bucket, objKey1, lockModeCompliance, retainUntil, *putResp.VersionId)
			requireS3Error(t, err, http.StatusForbidden, "AccessDenied")

			putResp, err = putObjectWithRetention(ctx, client, bucket, objKey1, lockModeCompliance, retainUntil)
			require.NoError(t, err)

			_, err = putObjectRetention(ctx, client, bucket, objKey1, lockModeGovernance, retainUntil, *putResp.VersionId)
			requireS3Error(t, err, http.StatusForbidden, "AccessDenied")
		})

		runRetentionModeTest("changing retention period", func(t *testing.T, mode string) {
			putResp, err := putObjectWithRetention(ctx, client, bucket, objKey1, mode, retainUntil)
			require.NoError(t, err)

			extendedRetainUntil := retainUntil.Add(10 * time.Minute)

			_, err = putObjectRetention(ctx, client, bucket, objKey1, mode, extendedRetainUntil, *putResp.VersionId)
			require.NoError(t, err)

			objInfo, err := getObject(ctx, client, bucket, objKey1, "")
			require.NoError(t, err)
			require.WithinDuration(t, extendedRetainUntil, *objInfo.ObjectLockRetainUntilDate, time.Minute)

			_, err = putObjectRetention(ctx, client, bucket, objKey1, mode, extendedRetainUntil.Add(-time.Minute), *putResp.VersionId)
			requireS3Error(t, err, http.StatusForbidden, "AccessDenied")

			_, err = putObjectRetentionBypassGovernance(ctx, client, bucket, "nonexistent", mode, extendedRetainUntil.Add(-time.Hour), *putResp.VersionId)
			requireS3Error(t, err, http.StatusBadRequest, "InvalidArgument")

			if mode == lockModeGovernance {
				_, err = putObjectRetentionBypassGovernance(ctx, client, bucket, objKey1, mode, extendedRetainUntil.Add(-time.Minute), *putResp.VersionId)
				require.NoError(t, err)
			}
		})

		runRetentionModeTest("bypass governance remove retention", func(t *testing.T, mode string) {
			putResp, err := putObjectWithRetention(ctx, client, bucket, objKey1, mode, retainUntil)
			require.NoError(t, err)

			_, err = putObjectRetention(ctx, client, bucket, objKey1, "", time.Time{}, *putResp.VersionId)
			requireS3Error(t, err, http.StatusBadRequest, "InvalidArgument")

			_, err = putObjectRetentionBypassGovernance(ctx, client, bucket, objKey1, "", time.Time{}, *putResp.VersionId)
			if mode == lockModeGovernance {
				require.NoError(t, err)
				require.NoError(t, deleteObject(ctx, client, bucket, objKey1, *putResp.VersionId))
			} else {
				requireS3Error(t, err, http.StatusForbidden, "AccessDenied")
			}
		})

		runRetentionModeTest("object lock settings in object info", func(t *testing.T, mode string) {
			putResp, err := putObjectWithRetention(ctx, client, bucket, objKey1, mode, retainUntil)
			require.NoError(t, err)

			objInfo, err := getObject(ctx, client, bucket, objKey1, *putResp.VersionId)
			require.NoError(t, err)
			require.Equal(t, putResp.VersionId, objInfo.VersionId)
			require.Equal(t, mode, *objInfo.ObjectLockMode)
			require.WithinDuration(t, retainUntil, *objInfo.ObjectLockRetainUntilDate, time.Minute)

			_, err = getObjectRetention(ctx, client, bucket, "nonexistent", "")
			requireS3Error(t, err, http.StatusNotFound, "NoSuchKey")

			retResp, err := getObjectRetention(ctx, client, bucket, objKey1, *putResp.VersionId)
			require.NoError(t, err)
			require.Equal(t, mode, *retResp.Retention.Mode)
			require.WithinDuration(t, retainUntil, *retResp.Retention.RetainUntilDate, time.Minute)
		})

		runRetentionModeTest("delete locked object version", func(t *testing.T, mode string) {
			putResp, err := putObjectWithRetention(ctx, client, bucket, objKey1, mode, retainUntil)
			require.NoError(t, err)

			requireS3Error(t, deleteObject(ctx, client, bucket, objKey1, *putResp.VersionId), http.StatusForbidden, "AccessDenied")

			mpResp, err := putObjectMultipartWithRetention(ctx, client, bucket, objKey2, mode, retainUntil)
			require.NoError(t, err)

			requireS3Error(t, deleteObject(ctx, client, bucket, objKey2, *mpResp.VersionId), http.StatusForbidden, "AccessDenied")

			if mode == lockModeGovernance {
				require.NoError(t, deleteObjectBypassGovernance(ctx, client, bucket, objKey1, *putResp.VersionId))
				require.NoError(t, deleteObjectBypassGovernance(ctx, client, bucket, objKey2, *mpResp.VersionId))
			}
		})

		t.Run("invalid object state delete marker", func(t *testing.T) {
			_, err := putObjectWithRetention(ctx, client, bucket, objKey1, lockModeCompliance, retainUntil)
			require.NoError(t, err)

			require.NoError(t, deleteObject(ctx, client, bucket, objKey1, ""))

			_, err = getObjectRetention(ctx, client, bucket, objKey1, "")
			requireS3Error(t, err, http.StatusMethodNotAllowed, "MethodNotAllowed")

			_, err = putObjectRetention(ctx, client, bucket, objKey1, lockModeCompliance, retainUntil, "")
			requireS3Error(t, err, http.StatusMethodNotAllowed, "MethodNotAllowed")

			_, err = getObjectLegalHold(ctx, client, bucket, objKey1, "")
			requireS3Error(t, err, http.StatusMethodNotAllowed, "MethodNotAllowed")

			_, err = putObjectLegalHold(ctx, client, bucket, objKey1, legalHoldOn, "")
			requireS3Error(t, err, http.StatusMethodNotAllowed, "MethodNotAllowed")
		})

		runRetentionModeTest("copy object", func(t *testing.T, mode string) {
			noLockBucket := testrand.BucketName()
			require.NoError(t, createBucket(ctx, client, noLockBucket, true, false))

			putResp, err := putObjectWithRetention(ctx, client, bucket, objKey1, mode, retainUntil)
			require.NoError(t, err)

			_, err = copyObjectWithRetention(ctx, client, bucket, objKey1, *putResp.VersionId, noLockBucket, objKey2, mode, &retainUntil)
			requireS3Error(t, err, http.StatusBadRequest, "InvalidRequest")

			_, err = putObjectLegalHold(ctx, client, bucket, objKey1, legalHoldOn, *putResp.VersionId)
			require.NoError(t, err)

			copyResp, err := copyObject(ctx, client, bucket, objKey1, *putResp.VersionId, bucket, objKey2)
			require.NoError(t, err)

			_, err = getObjectRetention(ctx, client, bucket, objKey2, *copyResp.VersionId)
			requireS3Error(t, err, http.StatusNotFound, "NoSuchObjectLockConfiguration")

			objInfo, err := getObject(ctx, client, bucket, objKey2, "")
			require.NoError(t, err)
			require.Nil(t, objInfo.ObjectLockMode)
			require.Nil(t, objInfo.ObjectLockRetainUntilDate)
			require.Equal(t, legalHoldOff, *objInfo.ObjectLockLegalHoldStatus)

			require.NoError(t, deleteObject(ctx, client, bucket, objKey2, *copyResp.VersionId))

			copyResp, err = copyObjectWithRetention(ctx, client, bucket, objKey1, *putResp.VersionId, bucket, objKey3, mode, &retainUntil)
			require.NoError(t, err)

			getResp, err := getObject(ctx, client, bucket, objKey3, *copyResp.VersionId)
			require.NoError(t, err)
			require.Equal(t, mode, *getResp.ObjectLockMode)
			require.WithinDuration(t, retainUntil, *getResp.ObjectLockRetainUntilDate, time.Minute)

			requireS3Error(t, deleteObject(ctx, client, bucket, objKey3, *copyResp.VersionId), http.StatusForbidden, "AccessDenied")

			copyResp, err = copyObjectWithLegalHoldAndRetention(ctx, client, bucket, objKey1, *putResp.VersionId, bucket, objKey2, legalHoldOn, mode, &retainUntil)
			require.NoError(t, err)

			getResp, err = getObject(ctx, client, bucket, objKey2, *copyResp.VersionId)
			require.NoError(t, err)
			require.Equal(t, mode, *getResp.ObjectLockMode)
			require.WithinDuration(t, retainUntil, *getResp.ObjectLockRetainUntilDate, time.Minute)
			require.Equal(t, legalHoldOn, *getResp.ObjectLockLegalHoldStatus)
		})

		t.Run("copy object default retention", func(t *testing.T) {
			srcBucket, dstBucket := testrand.BucketName(), testrand.BucketName()
			require.NoError(t, createBucket(ctx, client, srcBucket, false, false))
			require.NoError(t, createBucket(ctx, client, dstBucket, true, true))

			defaultDays := int64(5)
			defaultMode := s3.ObjectLockRetentionModeCompliance

			_, err := putObjectLockConfiguration(ctx, client, dstBucket, "Enabled", &s3.ObjectLockRule{
				DefaultRetention: &s3.DefaultRetention{
					Days: &defaultDays,
					Mode: &defaultMode,
				},
			})
			require.NoError(t, err)

			_, err = putObject(ctx, client, srcBucket, objKey1, nil)
			require.NoError(t, err)

			copyResp, err := copyObject(ctx, client, srcBucket, objKey1, "", dstBucket, objKey1)
			require.NoError(t, err)

			objInfo, err := getObject(ctx, client, dstBucket, objKey1, *copyResp.VersionId)
			require.NoError(t, err)

			require.Equal(t, defaultMode, *objInfo.ObjectLockMode)
			require.WithinDuration(t, time.Now().AddDate(0, 0, int(defaultDays)), *objInfo.ObjectLockRetainUntilDate, time.Minute)

			copyResp, err = copyObjectWithRetention(ctx, client, srcBucket, objKey1, "", dstBucket, objKey2, s3.ObjectLockModeCompliance, &retainUntil)
			require.NoError(t, err)

			objInfo, err = getObject(ctx, client, dstBucket, objKey2, *copyResp.VersionId)
			require.NoError(t, err)

			require.Equal(t, s3.ObjectLockModeCompliance, *objInfo.ObjectLockMode)
			require.WithinDuration(t, retainUntil, *objInfo.ObjectLockRetainUntilDate, time.Minute)
		})

		runRetentionModeTest("mixed locked and unlocked versions", func(t *testing.T, mode string) {
			lockedPutResp, err := putObjectWithRetention(ctx, client, bucket, objKey1, mode, retainUntil)
			require.NoError(t, err)

			lockedGetResp, err := getObject(ctx, client, bucket, objKey1, *lockedPutResp.VersionId)
			require.NoError(t, err)
			require.Equal(t, mode, *lockedGetResp.ObjectLockMode)
			require.WithinDuration(t, retainUntil, *lockedGetResp.ObjectLockRetainUntilDate, time.Minute)

			unlockedPutResp, err := putObject(ctx, client, bucket, objKey1, nil)
			require.NoError(t, err)

			unlockedGetResp, err := getObject(ctx, client, bucket, objKey1, *unlockedPutResp.VersionId)
			require.NoError(t, err)
			require.Nil(t, unlockedGetResp.ObjectLockMode)
			require.Nil(t, unlockedGetResp.ObjectLockRetainUntilDate)

			legalHoldPutResp, err := putObjectWithLegalHold(ctx, client, bucket, objKey1, legalHoldOn)
			require.NoError(t, err)

			legalHoldGetResp, err := getObject(ctx, client, bucket, objKey1, *legalHoldPutResp.VersionId)
			require.NoError(t, err)
			require.Nil(t, legalHoldGetResp.ObjectLockMode)
			require.Nil(t, legalHoldGetResp.ObjectLockRetainUntilDate)
			require.Equal(t, legalHoldOn, *legalHoldGetResp.ObjectLockLegalHoldStatus)

			requireS3Error(t, deleteObject(ctx, client, bucket, objKey1, *lockedPutResp.VersionId), http.StatusForbidden, "AccessDenied")
			require.NoError(t, deleteObject(ctx, client, bucket, objKey1, *unlockedPutResp.VersionId))
			requireS3Error(t, deleteObject(ctx, client, bucket, objKey1, *legalHoldPutResp.VersionId), http.StatusForbidden, "AccessDenied")
		})

		t.Run("multi delete with locked and unlocked versions", func(t *testing.T) {
			putResp1, err := putObject(ctx, client, bucket, objKey1, nil)
			require.NoError(t, err)

			putResp2, err := putObject(ctx, client, bucket, objKey2, nil)
			require.NoError(t, err)

			obj1Version := putResp1.VersionId
			obj2Version := putResp2.VersionId

			_, err = putObjectRetention(ctx, client, bucket, objKey1, lockModeGovernance, retainUntil, "")
			require.NoError(t, err)

			deleteResp, err := client.DeleteObjectsWithContext(ctx, &s3.DeleteObjectsInput{
				Bucket: new(bucket),
				Delete: &s3.Delete{
					Objects: []*s3.ObjectIdentifier{
						{
							Key:       new(objKey1),
							VersionId: obj1Version,
						},
						{
							Key:       new(objKey2),
							VersionId: obj2Version,
						},
					},
				},
			})
			require.NoError(t, err)

			require.Len(t, deleteResp.Deleted, 1)
			require.Len(t, deleteResp.Errors, 1)

			require.Equal(t, objKey2, *deleteResp.Deleted[0].Key)
			require.Equal(t, obj2Version, deleteResp.Deleted[0].VersionId)

			require.Equal(t, "AccessDenied", *deleteResp.Errors[0].Code)
			require.Equal(t, objKey1, *deleteResp.Errors[0].Key)
			require.Equal(t, obj1Version, deleteResp.Errors[0].VersionId)

			deleteResp, err = client.DeleteObjectsWithContext(ctx, &s3.DeleteObjectsInput{
				Bucket: new(bucket),
				Delete: &s3.Delete{
					Objects: []*s3.ObjectIdentifier{
						{
							Key:       new(objKey1),
							VersionId: obj1Version,
						},
					},
				},
				BypassGovernanceRetention: new(true),
			})
			require.NoError(t, err)

			require.Len(t, deleteResp.Deleted, 1)
			require.Len(t, deleteResp.Errors, 0)

			require.Equal(t, objKey1, *deleteResp.Deleted[0].Key)
			require.Equal(t, obj1Version, deleteResp.Deleted[0].VersionId)
		})
	})
}

func getObjectLockConfiguration(ctx context.Context, client *s3.S3, bucket string) (*s3.GetObjectLockConfigurationOutput, error) {
	return client.GetObjectLockConfigurationWithContext(ctx, &s3.GetObjectLockConfigurationInput{
		Bucket: new(bucket),
	})
}

func putObjectLockConfiguration(ctx context.Context, client *s3.S3, bucket, enabledStatus string, rule *s3.ObjectLockRule) (*s3.PutObjectLockConfigurationOutput, error) {
	return client.PutObjectLockConfigurationWithContext(ctx, &s3.PutObjectLockConfigurationInput{
		Bucket: new(bucket),
		ObjectLockConfiguration: &s3.ObjectLockConfiguration{
			ObjectLockEnabled: new(enabledStatus),
			Rule:              rule,
		},
	})
}

func putObjectLegalHold(ctx context.Context, client *s3.S3, bucket, key, status, versionID string) (*s3.PutObjectLegalHoldOutput, error) {
	input := s3.PutObjectLegalHoldInput{
		Bucket: new(bucket),
		Key:    new(key),
		LegalHold: &s3.ObjectLockLegalHold{
			Status: new(status),
		},
	}
	if versionID != "" {
		input.VersionId = new(versionID)
	}
	return client.PutObjectLegalHoldWithContext(ctx, &input)
}

func getObjectLegalHold(ctx context.Context, client *s3.S3, bucket, key, versionID string) (*s3.GetObjectLegalHoldOutput, error) {
	input := s3.GetObjectLegalHoldInput{
		Bucket: new(bucket),
		Key:    new(key),
	}
	if versionID != "" {
		input.VersionId = new(versionID)
	}
	return client.GetObjectLegalHoldWithContext(ctx, &input)
}

func putObjectWithRetention(ctx context.Context, client *s3.S3, bucket, key, mode string, retainUntil time.Time) (*s3.PutObjectOutput, error) {
	return client.PutObjectWithContext(ctx, &s3.PutObjectInput{
		Bucket:                    new(bucket),
		Key:                       new(key),
		ObjectLockMode:            new(mode),
		ObjectLockRetainUntilDate: new(retainUntil),
	})
}

func putObjectMultipartWithRetention(ctx context.Context, client *s3.S3, bucket, key, mode string, retainUntil time.Time) (*s3.CompleteMultipartUploadOutput, error) {
	upload, err := client.CreateMultipartUploadWithContext(ctx, &s3.CreateMultipartUploadInput{
		Bucket:                    new(bucket),
		Key:                       new(key),
		ObjectLockMode:            new(mode),
		ObjectLockRetainUntilDate: new(retainUntil),
	})
	if err != nil {
		return nil, err
	}

	part, err := client.UploadPartWithContext(ctx, &s3.UploadPartInput{
		Bucket:     new(bucket),
		Key:        new(key),
		UploadId:   upload.UploadId,
		PartNumber: new(int64(1)),
		Body:       bytes.NewReader(testrand.Bytes(memory.KiB)),
	})
	if err != nil {
		return nil, err
	}

	return client.CompleteMultipartUploadWithContext(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   new(bucket),
		Key:      new(key),
		UploadId: upload.UploadId,
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: []*s3.CompletedPart{
				{
					ETag:       part.ETag,
					PartNumber: new(int64(1)),
				},
			},
		},
	})
}

func putObjectMultipartWithLegalHold(ctx context.Context, client *s3.S3, bucket, key, legalHoldStatus string) (*s3.CompleteMultipartUploadOutput, error) {
	upload, err := client.CreateMultipartUploadWithContext(ctx, &s3.CreateMultipartUploadInput{
		Bucket:                    new(bucket),
		Key:                       new(key),
		ObjectLockLegalHoldStatus: new(legalHoldStatus),
	})
	if err != nil {
		return nil, err
	}

	part, err := client.UploadPartWithContext(ctx, &s3.UploadPartInput{
		Bucket:     new(bucket),
		Key:        new(key),
		UploadId:   upload.UploadId,
		PartNumber: new(int64(1)),
		Body:       bytes.NewReader(testrand.Bytes(memory.KiB)),
	})
	if err != nil {
		return nil, err
	}

	return client.CompleteMultipartUploadWithContext(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   new(bucket),
		Key:      new(key),
		UploadId: upload.UploadId,
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: []*s3.CompletedPart{
				{
					ETag:       part.ETag,
					PartNumber: new(int64(1)),
				},
			},
		},
	})
}

func putObjectWithLegalHold(ctx context.Context, client *s3.S3, bucket, key, legalHoldStatus string) (*s3.PutObjectOutput, error) {
	return client.PutObjectWithContext(ctx, &s3.PutObjectInput{
		Bucket:                    new(bucket),
		Key:                       new(key),
		ObjectLockLegalHoldStatus: new(legalHoldStatus),
	})
}

func putObjectWithLegalHoldAndRetention(ctx context.Context, client *s3.S3, bucket, key, legalHoldStatus, mode string, retainUntil time.Time) (*s3.PutObjectOutput, error) {
	return client.PutObjectWithContext(ctx, &s3.PutObjectInput{
		Bucket:                    new(bucket),
		Key:                       new(key),
		ObjectLockLegalHoldStatus: new(legalHoldStatus),
		ObjectLockMode:            new(mode),
		ObjectLockRetainUntilDate: new(retainUntil),
	})
}

func getObjectRetention(ctx context.Context, client *s3.S3, bucket, key, versionID string) (*s3.GetObjectRetentionOutput, error) {
	input := s3.GetObjectRetentionInput{
		Bucket: new(bucket),
		Key:    new(key),
	}
	if versionID != "" {
		input.VersionId = new(versionID)
	}
	return client.GetObjectRetentionWithContext(ctx, &input)
}

func putObjectRetention(ctx context.Context, client *s3.S3, bucket, key, lockMode string, retainUntil time.Time, versionID string) (*s3.PutObjectRetentionOutput, error) {
	input := s3.PutObjectRetentionInput{
		Bucket:    new(bucket),
		Key:       new(key),
		Retention: &s3.ObjectLockRetention{},
	}
	if versionID != "" {
		input.VersionId = new(versionID)
	}
	if lockMode != "" {
		input.Retention.Mode = new(lockMode)
	}
	if !retainUntil.IsZero() {
		input.Retention.RetainUntilDate = new(retainUntil)
	}
	return client.PutObjectRetentionWithContext(ctx, &input)
}

func putObjectRetentionBypassGovernance(ctx context.Context, client *s3.S3, bucket, key, lockMode string, retainUntil time.Time, versionID string) (*s3.PutObjectRetentionOutput, error) {
	input := s3.PutObjectRetentionInput{
		Bucket:                    new(bucket),
		Key:                       new(key),
		BypassGovernanceRetention: new(true),
		Retention:                 &s3.ObjectLockRetention{},
	}
	if versionID != "" {
		input.VersionId = new(versionID)
	}
	if lockMode != "" {
		input.Retention.Mode = new(lockMode)
	}
	if !retainUntil.IsZero() {
		input.Retention.RetainUntilDate = new(retainUntil)
	}
	return client.PutObjectRetentionWithContext(ctx, &input)
}

func copyObjectWithRetention(ctx context.Context, client *s3.S3, sourceBucket, sourceKey, sourceVersionID, destBucket, destKey, lockMode string, retainUntil *time.Time) (*s3.CopyObjectOutput, error) {
	return client.CopyObjectWithContext(ctx, &s3.CopyObjectInput{
		Bucket:                    new(destBucket),
		Key:                       new(destKey),
		CopySource:                new(copySource(sourceBucket, sourceKey, sourceVersionID)),
		ObjectLockMode:            new(lockMode),
		ObjectLockRetainUntilDate: retainUntil,
	})
}

func copyObjectWithLegalHoldAndRetention(ctx context.Context, client *s3.S3, sourceBucket, sourceKey, sourceVersionID, destBucket, destKey, legalHoldStatus, lockMode string, retainUntil *time.Time) (*s3.CopyObjectOutput, error) {
	return client.CopyObjectWithContext(ctx, &s3.CopyObjectInput{
		Bucket:                    new(destBucket),
		Key:                       new(destKey),
		CopySource:                new(copySource(sourceBucket, sourceKey, sourceVersionID)),
		ObjectLockLegalHoldStatus: new(legalHoldStatus),
		ObjectLockMode:            new(lockMode),
		ObjectLockRetainUntilDate: retainUntil,
	})
}

func deleteObjectBypassGovernance(ctx context.Context, client *s3.S3, bucket, key, versionID string) error {
	input := s3.DeleteObjectInput{
		Bucket:                    new(bucket),
		Key:                       new(key),
		BypassGovernanceRetention: new(true),
	}
	if versionID != "" {
		input.VersionId = new(versionID)
	}
	_, err := client.DeleteObjectWithContext(ctx, &input)
	return err
}
