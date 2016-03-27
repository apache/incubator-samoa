---
title: Apache SAMOA for MOA users
layout: documentation
documentation: true
---
If you're an advanced user of [MOA](http://moa.cms.waikato.ac.nz/), you'll find easy to run SAMOA. You need to note the following:

* There is no GUI interface in SAMOA
* You can run SAMOA in the following modes:
   1. Simulation Environment. Use `org.apache.samoa.DoTask` instead of `moa.DoTask`   
   2. Storm Local Mode. Use `org.apache.samoa.LocalStormDoTask` instead of `moa.DoTask`
   3. Storm Cluster Mode. You need to use the `samoa` script as it is explained in [Executing SAMOA with Apache Storm](Executing SAMOA with Apache Storm).
   4. S4. You need to use the `samoa` script as it is explained in [Executing SAMOA with Apache S4](Executing SAMOA with Apache S4)

To start with SAMOA, you can start with a simple example using the CoverType dataset as it is discussed in [Getting Started](Getting Started).  

To use MOA algorithms inside SAMOA, take a look at [https://github.com/samoa-moa/samoa-moa](https://github.com/samoa-moa/samoa-moa). 
