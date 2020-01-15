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
allowing developers to focus instead on their application logic. Apache Twill allows you to use YARN’s distributed
capabilities with a programming model that is similar to running threads.

### Why do I need Apache Twill?

Apache Twill dramatically simplifies and reduces development efforts, enabling you to quickly and
easily both develop and manage distributed applications through its simple abstraction layer on top of YARN.
YARN, although originally designed for MapReduce v2, can be used as a generic cluster resource management framework
that can run almost any type of application on a Hadoop® cluster. However, with its powerful capabilities, YARN can 
introduce complexities for developers. 

In contrast, Twill's abstraction model over YARN closely resembles the Java thread model, with which many developers are
familiar. Moreover, Twill provides application lifecycle management, service discovery, distributed process coordination, and resiliency to failure, which are required by many distributed applications.

Apache Twill allows you to develop, deploy, and manage your distributed applications with a simpler programming model,
with rich built-in features for solving common distributed-application problems. Whether you are a developer or an
operating engineer, you will find Apache Twill helps you greatly reduce the effort in developing and operating your
applications on a Hadoop® cluster.

### Latest Release

The latest release of Apache Twill is [0.13.0](releases/0.13.0.html). 
Please go to the [release](releases/0.13.0.html) page for additional information.

### Is it Building?

Status of CI build at Travis CI: [![Build Status](https://travis-ci.org/apache/twill.svg?branch=master)](https://travis-ci.org/apache/twill)

### Apache Twill Presentation in Apache: Big Data North America 2016

<iframe src="http://www.slideshare.net/slideshow/embed_code/key/2bCOzXHycldrtP" width="595" height="373" frameborder="0" marginwidth="0" marginheight="0" scrolling="no" style="border:1px solid #CCC; border-width:1px; margin-bottom:5px; max-width: 100%;" allowfullscreen="true"> </iframe>

### Apache Twill Presentation in ApacheCon 2014

<iframe src="http://www.slideshare.net/slideshow/embed_code/33789812" width="427" height="356" frameborder="0" marginwidth="0" marginheight="0" scrolling="no" style="border:1px solid #CCC; border-width:1px 1px 0; margin-bottom:5px; max-width: 100%;" allowfullscreen="true">
</iframe>
