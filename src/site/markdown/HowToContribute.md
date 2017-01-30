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
  <title>How to Contribute</title>
</head>

## Contributing to Apache Twill

The Apache Twill team welcome all types of contributions, whether they are bug reports, feature requests,
documentation, or code patches.

### Reporting Issues

To report bugs or request new features, please open an issue in the
[Apache Twill JIRA](https://issues.apache.org/jira/browse/TWILL). You can also use the
[dev mailing list](mail-lists.html) for general questions or discussions.

### Contributing Code

We prefer contributions through [GitHub](https://github.com/apache/twill) pull requests. Please follow
these steps to get your contributions in:

1. Open a new issue or pick up an existing one in the [Apache Twill JIRA](https://issues.apache.org/jira/browse/TWILL)
  about the patch that you are going to submit.
2. If you are proposing public API changes or big changes, please attach a design document to the JIRA. You
  can also use the [dev mailing list](mail-lists.html) to discuss it first. This will help us understand your needs
  and best guide your solution in a way that fits the project.
3. [Fork](https://help.github.com/articles/fork-a-repo) the
  [Apache Twill GitHub repo.](https://github.com/apache/twill)
4. Make the changes and send a [pull request](https://help.github.com/articles/using-pull-requests) from your
  forked repo to the Apache Twill repo.
5. Please prefix your pull request title with the JIRA issue ID; for example, `(TWILL-87) Adding container placement policy`.
6. Please complete the pull request description with additional details as appropriate.
7. Once sent, code review will be done through the pull request.
8. Once all review issues are resolved, we will merge the changes into the `master` branch of the Apache Twill repo.

### Coding Style

* IntelliJ IDEA 2016 [settings](twill-idea-settings.jar)
* Eclipse IDE Neon [settings](twill-eclipse-settings.epf)

### How to Merge Code Changes

Committer can merge code changes that are already reviewed into the `master` branch with the following steps:

1. Make sure the GitHub pull request is squashed into one commit. If not, ask the patch contributor to help doing so.
        
2. Download the patch file from GitHub. You can append `.patch` to the end of the GitHub pull request URL to get the patch file.

        curl -L -O https://github.com/apache/twill/pull/${PR_NUMBER}.patch
3. Edit the patch file and add the following line in the commit message for closing the pull request.

        This closes #${PR_NUMBER} from GitHub.
4. Apply the patch and push it back to remote repo. Make sure you apply it on the latest `master` branch.

        git checkout master
        git pull origin master
        git am --signoff < ${PR_NUMBER}
        git push origin master
5. Close the JIRA issue associated with the patch.