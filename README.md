​
# jadehadoophdfs
JADE agent that communicates with HADOOP's namenode

In order to use this code, several requirements need to be fulfilled:
- Configured and running Hadoop cluster
- Configured and built JADE environment
- Configured log4j environment

Next, make the appropriate changes in the compilestart.sh script.
Specify paths to jade home directory, the path to where the *.java classes are located
Additionally, specify the home directory of Hadoop, and the location of it's core-site.xml file

To run the JADE platform, run the script compilestart.sh

The agent communication is as follows.
HDFS agent (HDFSWriterAgent) needs to be started first. It is already started in the compilestart.sh script, alongside the JADE platform.
It is preferable to wait for the message from the agent that the file system is configured before starting the sender agent (SenderAgent).
The sender agent can be started from JADE GUI, arguments that are needed to start the agent should be in a format <inputpath>:<outputpath> or <inputpath>;<outputpath>, where the <inputpath> it the path to the file that needs to be copied, and <outputfile> is a HDFS path.
The execution mimics the copyFromLocal HDFS command, where everything is done using the combination of JADE and HADOOP APIs.

As for the agent communication protocols, it goes in the following way:
Sender searchs for HDFS agents. It needs to find exactly one agent. If no agents or more HDFS agents are found, it terminates.
If HDFS agent found, a call for proposal, CFP is sent to it, with the output path.
The HDFS checks whether the path already exists, and if not, sends PROPOSE.
If the path already exists, it sends REFUSE, which terminates the sender agent.
If the PROPOSE message is received, sender replies with ACCEPT_PROPOSAL and the input file path in the content.
The HDFS agent receives the message, copies the file, and informs the sender agent about it.

Additionally, in the takeDown() method of the HDFSWriterAgent there is a procedure to call the fsck command, with the following arguments:
- directory of <outputpath>
- files
- blocs
- racks
- 

 The fsck check is done when the agent is terminating (For example, executing Kill agent from the JADE GUI).
