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

Apache Twill Site Update Instructions
-------------------------------------

1. Publish snapshot artifacts to maven

        git checkout master

        mvn clean deploy -DskipTests -Dremoteresources.skip=true -P hadoop-2.6 -P apache-release
1. Build javadocs for the newly released version

        git checkout branch-${RELEASE_VERSION}

        mvn javadoc:aggregate
1. Copy the javadocs generated in previous step to site directory

        git checkout site
        
        cp -r target/site/apidocs src/site/resources/apidocs-${RELEASE_VERSION}
1. Update release version and link in `src/site/markdown/index.md`
1. Create new release page markdown file `src/site/markdown/release/${RELEASE_VERSION}.md`.
   You can base on the previous release page and update accordingly.
1. Update the `src/site/site.xml` file to add new release information.
1. Build the site

        mvn clean site -DskipTests -P site
   All Twill website files will be generated at the `target/site` directory
1. Update and check-in changes in SVN `https://svn.apache.org/repos/asf/twill/site`.
        
