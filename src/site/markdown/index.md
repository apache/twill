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
  <title>Home</title>
</head>

### What is Apache Twill?

Apache Twill is an abstraction over Apache Hadoop® YARN that reduces the complexity of developing distributed applications,
allowing developers to focus more on their application logic. Apache Twill allows you to use YARN’s distributed capabilities
with a programming model that is similar to running threads.

### Why do I need Apache Twill?

Apache Twill dramatically simplifies and reduces development efforts, enabling you to quickly and
easily develop and manage distributed applications through its simple abstraction layer on top of YARN.
YARN, although originally designed for MapReduce v2, can be used as a generic cluster resource management framework
that can run almost any type of applications on a Hadoop® cluster. Given it's power, however, YARN can be quite difficult to use and
requires a big ramp up effort since YARN's capabilities are quite low level and the learning curve is really steep.
Moreover, many distributed applications have common needs such as application lifecycle management, service discovery,
distributed process coordination and resiliency to failure, which are not out of the box features from YARN.

Apache Twill allows you to develop, deploy and manage your distributed applications with a much simpler programming model,
with rich build-in features for common distributed applications needs. Whether you are a developer or operating engineer
will find Apache Twill helps you greatly reduces the effort in developing and operating your applications on a
Hadoop® cluster.

### Latest Release

The latest release of Apache Twill is [0.7.0-incubating](releases/0.7.0-incubating.html). 
Please go to the [release](releases/0.7.0-incubating.html) page
to find out more.

### Is it Building?

Status of CI build at Travis CI: [![Build Status](https://travis-ci.org/apache/incubator-twill.svg?branch=master)](https://travis-ci.org/apache/incubator-twill)

### Apache Twill Presentation in ApacheCon 2014

<iframe src="http://www.slideshare.net/slideshow/embed_code/33789812" width="427" height="356" frameborder="0" marginwidth="0" marginheight="0" scrolling="no" style="border:1px solid #CCC; border-width:1px 1px 0; margin-bottom:5px; max-width: 100%;" allowfullscreen="true">
</iframe>

### Disclaimer

Apache Twill is an effort undergoing incubation at The Apache Software Foundation (ASF), sponsored by Incubator.
Incubation is required of all newly accepted projects until a further review indicates that the infrastructure,
communications, and decision making process have stabilized in a manner consistent with other successful ASF projects.
While incubation status is not necessarily a reflection of the completeness or stability of the code,
it does indicate that the project has yet to be fully endorsed by the ASF.
