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

What is Apache Twill?
---------------------
[Twill] is an abstraction over Apache Hadoop® YARN that reduces the complexity 
of developing distributed applications, allowing developers to focus more on 
their business logic. Twill allows you to use YARN’s distributed capabilities 
with a programming model that is similar to running threads.


Getting Started
---------------
You can build and install the Apache Twill by:

```sh
    git clone https://git-wip-us.apache.org/repos/asf/twill.git
    cd twill
    mvn install
```

After the maven installation completes, you can include the artifact 
org.apache.twill:twill-yarn as a dependency on your other projects.

Export Control
-------------
This distribution includes cryptographic software. The country in which you 
currently reside may have restrictions on the import, possession, use, and/or
re-export to another country, of encryption software. BEFORE using any 
encryption software, please check your country's laws, regulations and 
policies concerning the import, possession, or use, and re-export of encryption
software, to see if this is permitted. See <http://www.wassenaar.org/> for more
information.

The U.S. Government Department of Commerce, Bureau of Industry and Security 
(BIS), has classified this software as Export Commodity Control Number (ECCN) 
5D002.C.1, which includes information security software using or performing 
cryptographic functions with asymmetric algorithms. The form and manner of this
Apache Software Foundation distribution makes it eligible for export under the 
License Exception ENC Technology Software Unrestricted (TSU) exception (see the
BIS Export Administration Regulations, Section 740.13) for both object code and
source code.

The following provides more details on the included cryptographic software:

Apache Twill uses the built-in java cryptography libraries for unique ID
generation. See 
<http://www.oracle.com/us/products/export/export-regulations-345813.html>
for more details on Java's cryptography features.

[Twill]: http://twill.apache.org/
