# How to release a new version

## When to release a new version

New version should be released when we are ready to make changes generally available.

New version should not be released if we want to test our latest changes or to make them available to a limited number of users. This can be achieved without releasing a new version, e.g. by compiling the binary from latest main or specific Git commit. However, commits from main are not suitable for use in production unless they are a release tag.

Consider releasing a new Release Candidate version to make changes available to a larger group of users, if we are not ready to make them available to everyone yet.

Under no circumstances may releases be done during the weekend or the wee hours of the night.

## Version numbers

We follow the rules for semantic versioning, but prefixed with the letter `v`.

Examples of official releases:
- `v1.0.0`
- `v1.0.3`
- `v1.2.3`
- `v2.1.7`

Examples of Release Candidates:
- `v1.0.0-rc.4`
- `v2.1.0-rc.1`

## Step-by-step release process

1. Announce your intention to make a new release to the #team-edge Slack channel.
2. Wait for a confirmation by at least one maintainer of this project (storj/gateway) before proceeding with the next step.
3. Create a new release from the Github web interface:
  - Go to https://github.com/storj/gateway/releases.
  - Click the `Draft a new release` button.
  - Enter `Tag version` following the rules for the version number, e.g. `v1.2.3`.
  - Enter the same value as `Release title`, e.g. `v1.2.3`.
  - Describe the changes since the previous release in a human-readable way. Only those changes that affect users. No need to describe refactorings, etc.
  - Select the `This is a pre-release` checkbox. This checkbox must always be selected at this point, even if this is an official release. We will deselelect it after we upload the binaries to the release. Otherwise, the links to the binaries of the latest release will be broken in the documentation and other places.
  - Click the `Publish release` button.
4. Creating the release tag triggers the release build on the private Jenkins: https://build.storj.io/job/gateway/view/tags/
5. When the release build completes, the binaries will be uploaded to the staging storage repository: http://storj-v3-alpha-builds.storage.googleapis.com/index.html and Docker images will be published to Docker Hub: https://hub.docker.com/r/storjlabs/gateway
6. Find the build tag (e.g. `9b58a11-v1.0.1-go1.13.8`) and download all the binaries to your local drive.
7. Update the Github release:
  - Go to https://github.com/storj/gateway/releases.
  - Cick the `Edit` button on the release.
  - Upload all binaries as release artifacts.
  - Add the following line to the description:
    - >Docker image for this release: `storjlabs/gateway:<build-tag>`.
    - Example:
      - >Docker image for this release: `9b58a11-v1.0.1-go1.13.8`.
  - If this is an official release, deselect the `This is a pre-release` checkbox.
  - Click the `Update release` button.
