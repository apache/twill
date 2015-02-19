<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at
  
      http://www.apache.org/licenses/LICENSE-2.0
  
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

<head>
  <title>Release Guide</title>
</head>

## Releasing Apache Twill

This guide describes the steps to building and releasing Apache Twill artifacts.

### Environment Setup

#### Generate GPG keypair
Generate a GPG keypair for signing artifacts, if you don't already have one.
See [GPG key generation](http://www.apache.org/dev/openpgp.html#generate-key) on how to do so.
Make sure the public key is published to a public key server, as well as added to the Twill
[KEYS](https://dist.apache.org/repos/dist/release/incubator/twill/KEYS) file.

#### Generate an encrypted password for maven deploy
Generate an encrypted password for your apache id using maven
(see [Maven Encryption Guide](http://maven.apache.org/guides/mini/guide-encryption.html)) and
add the following section to your Maven settings (`~/.m2/settings.xml`):

```xml
<server>
  <id>apache.releases.https</id>
  <username>[APACHE_USER_ID]</username>
  <password>[ENCRYPTED_APACHE_PASSWORD]</password>
</server>
```

### Release procedure

#### Create a new release branch from master
```
git checkout -b branch-[RELEASE_VERSION]
```
The `[RELEASE_VERSION]` is something such as `0.5.0`.

#### Update the version to the non-snapshot version
```
mvn versions:set -DgenerateBackupPom=false -DnewVersion=[RELEASE_VERSION]-incubating
git commit . -m "Prepare for releasing [RELEASE_VERSION]-incubating"
```

#### Create a new signed tag for the release
```
git tag -s v[RELEASE_VERSION]-incubating -m 'Releasing [RELEASE_VERSION]-incubating'
```
  
#### Push both the new branch and the tag
```
git push origin branch-[RELEASE_VERSION]
git push origin v[RELEASE_VERSION]
```

#### Start gpg-agent
* Run `gpg-agent` to save the number of times that you have to type in your GPG password
  when building release artifacts.
* You can run `gpg -ab` to confirm the the agent is
  running and it has cached your password correctly.
* Alternatively, if you don't want to
  run GPG agent, you can specify your GPG password through
  `-Dgpg.passphrase=[GPG_PASSWORD]` when running Maven.
  
#### Build the source tarball and publish artifacts to the staging repo
```
mvn clean prepare-package -DskipTests -Dremoteresources.skip=true -P hadoop-2.0 &&
mvn prepare-package -DskipTests -Dremoteresources.skip=true -P hadoop-2.3 &&
mvn deploy -DskipTests -Dremoteresources.skip=true -P hadoopp-2.3 -P apache-release
```
The source tarball can be found in `target/apache-twill-[RELEASE_VERSION]-incubating-source-release.tar.gz`
after the above command has successfully completed.
  
#### Compute the MD5 and SHA512 of the source release tarball
```
cd target
md5 -q apache-twill-[RELEASE_VERSION]-incubating-source-release.tar.gz > apache-twill-[RELEASE_VERSION]-incubating-source-release.tar.gz.md5
shasum -a 512 apache-twill-[RELEASE_VERSION]-incubating-source-release.tar.gz > apache-twill-[RELEASE_VERSION]-incubating-source-release.tar.gz.sha512
```
  
#### Prepare release artifacts
1. Checkin the source release tarball, together with the signature, md5 and sha512 files
   to `dist.apache.org/repos/dist/dev/incubator/twill/[RELEASE_VERSION]-incubating-rc1/src/`
1. Create a `CHANGES.txt` file to describe the changes in the release and checkin the file
   to `dist.apache.org/repos/dist/dev/incubator/twill/[RELEASE_VERSION]-incubating-rc1/CHANGES.txt`
1. Go to [https://repository.apache.org](https://repository.apache.org) and close the staging repository.

#### Increase the version in master
```
git checkout master
git merge --no-ff branch-[RELEASE_VERSION]
mvn versions:set -DgenerateBackupPom=false -DnewVersion=[NEXT_RELEASE_VERSION]-incubating-SNAPSHOT
git commit . -m "Bump version to [NEXT_RELEASE_VERSION]-incubating-SNAPSHOT"
git push origin master
```

#### Vote for release in dev mailing list
Create a vote in the `dev@twill` mailing list and wait for 72 hours for the vote result.
Here is a template for the email:

```
Subject: [VOTE] Release of Apache Twill-[RELEASE_VERSION]-incubating [rc1]
==========================================================================

Hi all,

This is a call for a vote on releasing Apache Twill [RELEASE_VERSION]-incubating, release candidate 1. This
is the [Nth] release of Twill.

The source tarball, including signatures, digests, etc. can be found at:
https://dist.apache.org/repos/dist/dev/incubator/twill/[RELEASE_VERSION]-incubating-rc1/src

The tag to be voted upon is v[RELEASE_VERSION]-incubating:
https://git-wip-us.apache.org/repos/asf?p=incubator-twill.git;a=shortlog;h=refs/tags/v[RELEASE_VERSION]-incubating

The release hash is [REF]:
https://git-wip-us.apache.org/repos/asf?p=incubator-twill.git;a=commit;h=[REF]

The Nexus Staging URL:
https://repository.apache.org/content/repositories/orgapachetwill-[STAGE_ID]

Release artifacts are signed with the following key:
[URL_TO_SIGNER_PUBLIC_KEY]

KEYS file available:
https://dist.apache.org/repos/dist/dev/incubator/twill/KEYS

For information about the contents of this release, see:
https://dist.apache.org/repos/dist/dev/incubator/twill/[RELEASE_VERSION]-incubating-rc1/CHANGES.txt

Please vote on releasing this package as Apache Twill [RELEASE_VERSION]-incubating

The vote will be open for 72 hours.

[ ] +1 Release this package as Apache Twill [RELEASE_VERSION]-incubating
[ ] +0 no opinion
[ ] -1 Do not release this package because ...

Thanks,
[YOUR_NAME]
```

#### Consolidate vote result
After the vote is up for 72 hours and having at least three +1 binding votes and no -1
votes, close the vote by replying to the voting thread. Here is a template for the reply email:

```
Subject: [RESULT][VOTE] Release of Apache Twill-[RELEASE_VERSION]-incubating [rc1]
==================================================================================

Hi all,

After being opened for over 72 hours, the vote for releasing Apache Twill
[RELEASE_VERSION]-incubating passed with n binding +1s and no 0 or -1.

Binding +1s:
[BINDING_+1_NAMES]

I am going to create a vote in the general@ list.

Thanks,
[YOUR_NAME]
```

#### Vote for release from IPMC
1. Create a vote in the `general@` mailing list for the IPMC to vote for the
   release.
1. Wait for 72 hours for the vote result. Use the same template as the dev vote,
   with the addition of links to the dev vote and result mail thread.
1. After the vote in `general@` is completed with at least three +1 binding votes, close
   the vote by replying to the voting thread.

#### Finalize the release
1. Copy the release artifacts and `CHANGES.txt` from the dev to release directory at
   `dist.apache.org/repos/dist/release/incubator/twill/[RELEASE_VERSION]-incubating`
1. Go to [https://repository.apache.org](https://repository.apache.org) and release the
   staging repository.
1. Send out an announcement of the release to `dev@twill` and `announce@` mailing lists.
