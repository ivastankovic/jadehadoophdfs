package hadoop.agents;

import jade.core.Agent;
import jade.core.behaviours.CyclicBehaviour;
import jade.domain.DFService;
import jade.domain.FIPAException;
import jade.domain.FIPAAgentManagement.DFAgentDescription;
import jade.domain.FIPAAgentManagement.ServiceDescription;
import jade.lang.acl.ACLMessage;
import jade.lang.acl.MessageTemplate;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.tools.DFSck;
import org.apache.hadoop.io.IOUtils;

 /**
  * Agent which communicates with hadoop's name node and executes a copy of a local file (specified by localInputPath) to HDFS
  * The class needs a path to Hadoop's core-site XML.
  * When terminating, the HDFS agent performs a fsck check of the output path directory
*/
public class HDFSWriterAgent extends Agent{
	public static final String SERVICE_HDFS = "hdfscopy-local";
	public static final String SERVICE_NAME = "JADE-HADOOP";
	public static final String FS_PARAM_NAME = "fs.defaultFS";
	//public static final String PATH_TO_CORE_SITE_XML = "/usr/local/hadoop/etc/hadoop/core-site.xml";
	private String localInputPath;
	private Path outputPath;
	private Configuration conf;
	private FileSystem fs;
	private java.nio.file.Path path_to_core_site;
	
	protected void setup() {
		
		System.out.println("HDFS agent starting...");
		Object[] args = getArguments();
		if (args!=null && args.length ==1)
		{
			path_to_core_site = Paths.get((String)args[0]);
			if (Files.exists(path_to_core_site)){
				//configure HDFS
				conf = new Configuration();
				//note difference between org.apache.hadoop.fs.Path and java.nio.file.Path
		        conf.addResource(new Path(path_to_core_site.toString()));
		        
		        // introduced due to overriding dependencies
		        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
		       
		        
		        try{
		        	//accessing HDFS with the configuration
		        	fs = FileSystem.get(conf);
		        	System.out.println("configured filesystem = " + conf.get(FS_PARAM_NAME));
		        }
		        catch(IOException e){
		        	System.out.println("Unable to confiigure the filesystem");
		        	e.printStackTrace();
		        }
		        
				// Register the HDFS service in the yellow pages
				DFAgentDescription dfd = new DFAgentDescription();
				dfd.setName(getAID());
				ServiceDescription sd = new ServiceDescription();
				sd.setType(SERVICE_HDFS);
				sd.setName(SERVICE_NAME);
				dfd.addServices(sd);
				try {
					DFService.register(this, dfd);
				}
				catch (FIPAException fe) {
					fe.printStackTrace();
				}
		
				// Adding the behaviors serving file copy requests from sender agents
				
				// here it is checked if the specified output path doesn't already exist in HDFS
				addBehaviour(new FileRequestsServer());
				// copying files
				addBehaviour(new HDFSWriter());
			}
			else{
				// the input path doesn't exist, terminate agent
				System.out.println("Specified path to hadoop core-site.xml file doesn't exist."
						+ "Please Specify a valid path");
				doDelete();
			}
		}
		else{
			// Make the agent terminate
			System.out.println("Arguments not instantiated correctly."
					+ "Please provide path on local to Hadoop's config file core-site.xml");
			doDelete();
		}
	}
	
	protected void takeDown() {
		
		System.out.println("Taking down HDFS writer agent "+getAID().getName());
		try {
			
			//make sure file system is configured
			if(fs!=null){
				DFService.deregister(this);
				if(outputPath!=null){
					String dir= outputPath.getParent().toString();
					System.out.println("An fsck test will be performed on the directory " + dir);
					DFSck dfsck=new DFSck(conf);
				    String [] argums = {dir, "-files", "-blocks", "-racks"};
				    dfsck.run(argums);
				}
			}

		}
		catch (FIPAException fe) {
			fe.printStackTrace();
		}
		catch (IOException e){
			e.printStackTrace();
		}

		// Printout a dismissal message
		System.out.println("HDFSWriterAgent "+getAID().getName()+" terminating.");
	}
	/**
	 * Inner class FileRequestsServer
	 * class that checks if the output path from sender is available (doesn't already exist) in HDFS
	 */
	private class FileRequestsServer extends CyclicBehaviour {
		public void action() {
			MessageTemplate mt = MessageTemplate.MatchPerformative(ACLMessage.CFP);
			ACLMessage msg = myAgent.receive(mt);
			if (msg != null) {
				// CFP Message received. Process it
				String outputPathString = msg.getContent();
				outputPath = new Path(outputPathString);
				ACLMessage reply = msg.createReply();
				try{
					 if (!fs.exists(outputPath)) {
						reply.setPerformative(ACLMessage.PROPOSE);
						reply.setContent("ok");
					}
					else {
						// The path already exists
						reply.setPerformative(ACLMessage.REFUSE);
						reply.setContent("not-ok");
					}
					myAgent.send(reply);
				}
				catch(IOException e) {
		        	e.printStackTrace();
		        }
		  	}
			else {
				block();
			}
		}
	}  

	/**
	 * Inner class HDFSWriter.
	 * Used to copy the file to HDFS and notify the sender agent about it
	 */
	private class HDFSWriter extends CyclicBehaviour {
		public void action() {
			MessageTemplate mt = MessageTemplate.MatchPerformative(ACLMessage.ACCEPT_PROPOSAL);
			ACLMessage msg = myAgent.receive(mt);
			if (msg != null) {
				// ACCEPT_PROPOSAL Message received. Process it
				localInputPath = msg.getContent();
				ACLMessage reply = msg.createReply();
				try{
					System.out.println("Trying to copy file " + localInputPath + " to HDFS");
					OutputStream os = fs.create(outputPath);
			        InputStream is = new BufferedInputStream(new FileInputStream(localInputPath));
			        IOUtils.copyBytes(is, os, conf);
			        
			        //check if the file is copied and notify the sender
			        FileStatus filestat = fs.getFileStatus(outputPath);
			        if(filestat.isFile()){
			        	reply.setPerformative(ACLMessage.INFORM);
						reply.setContent("success");
			        }
			        else{
			        	reply.setPerformative(ACLMessage.FAILURE);
						reply.setContent("failed-copy");		
			        }
					myAgent.send(reply);
					}
				catch(IOException e){
					e.printStackTrace();
				}
			}
			else {
				block();
			}
		}
	}  
}
