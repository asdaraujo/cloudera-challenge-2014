This Java class comes from the mahout-integration artifact. At the time of
this implementation I was using CDH5 with YARN and Hadoop 2.0 and the Mahout
artifacts in the Maven repository were compiled with Hadoop 1.0 libraries
which were causing problems to my jobs.

I attempted to compile Mahout with the Hadoop 2.0 libraries but Mahout had
not been fully tested with those libraries and there were still bugs to fix.

So, in the end I copied the only class that I needed to my project to be 
able to read the input XML files.

When including the Mahout artifact, instead of copying the class source, the
project would compile but the mapreduce job would fail with the folliwing
error stack:

2014-06-28 02:47:08,650 FATAL [main] org.apache.hadoop.mapred.YarnChild: Error running child : java.lang.IncompatibleClassChangeError: Found interface org.apache.hadoop.mapreduce.TaskAttemptContext, but class was expected
        at org.apache.mahout.text.wikipedia.XmlInputFormat.createRecordReader(XmlInputFormat.java:52)
        at org.apache.crunch.impl.mr.run.CrunchRecordReader.initNextRecordReader(CrunchRecordReader.java:78)
        at org.apache.crunch.impl.mr.run.CrunchRecordReader.<init>(CrunchRecordReader.java:53)
        at org.apache.crunch.impl.mr.run.CrunchInputFormat.createRecordReader(CrunchInputFormat.java:77)
        at org.apache.hadoop.mapred.MapTask$NewTrackingRecordReader.<init>(MapTask.java:492)
        at org.apache.hadoop.mapred.MapTask.runNewMapper(MapTask.java:735)
        at org.apache.hadoop.mapred.MapTask.run(MapTask.java:340)
        at org.apache.hadoop.mapred.YarnChild$2.run(YarnChild.java:168)
        at java.security.AccessController.doPrivileged(Native Method)
        at javax.security.auth.Subject.doAs(Subject.java:415)
        at org.apache.hadoop.security.UserGroupInformation.doAs(UserGroupInformation.java:1548)
        at org.apache.hadoop.mapred.YarnChild.main(YarnChild.java:163)

