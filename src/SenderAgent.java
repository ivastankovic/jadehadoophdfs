package hadoop.agents;

import jade.core.Agent;
import jade.core.AID;
import jade.core.behaviours.*;
import jade.lang.acl.ACLMessage;
import jade.lang.acl.MessageTemplate;
import jade.domain.DFService;
import jade.domain.FIPAException;
import jade.domain.FIPAAgentManagement.DFAgentDescription;
import jade.domain.FIPAAgentManagement.ServiceDescription;

import java.nio.file.*;
import java.util.regex.Pattern;


/**
 * Sender agents uses 2 arguments,when instantiating
 * Similarly to HDFS -copyFromLocal, it needs an input path from the local filesystem
 * and an output path 
 */
public class SenderAgent extends Agent {
	// input path is the path to a file on the local file system
	//output path is the path to the file on HDFS (to be created)
	private String inputPath, outputPath;
	// reference to an HDFS agent
	private AID hdfsAgent;
	

	protected void setup() {
		System.out.println("Agent "+getAID().getName()+" starting.");

		Object[] arguments = getArguments();
		// there is a problem when passing multiple arguments if starting an agent through JADE GUI
		// preprocessing required before proceeding
		String[] args = null;
		
		if (arguments!=null)
		{
			// called from GUI
			// proces both colon and semicolon separators
			if(arguments.length==1){
				if(arguments[0].toString().contains(":")){
					args = arguments[0].toString().split(Pattern.quote(":"));
				}
				else if(arguments[0].toString().contains(";")){
					args = arguments[0].toString().split(Pattern.quote(";"));
				}
			}
			else if (arguments.length==2)
			{
				args = new String[2];
				args[0] = (String)arguments[0];
				args[1] = (String)arguments[1];

			}
		}

		
		if (args != null && args.length == 2) {
			
			System.out.println("Agent arguments: input path " + args[0] + " output path: " + args[1]);
			
			//inputPath = "/usr/trdata/test.txt";
			inputPath = args[0];
			
			//checking if the file exists on the local filesystem
			Path input = Paths.get(inputPath);
			if (!Files.exists(input)){
			// the input path doesn't exist, terminate agent
				System.out.println("Input file path doesn't exist.Please Specify a valid input path");
				doDelete();
			}
			
			//outputPath = "/dmkm/jade"+System.currentTimeMillis()+ ".txt";
			outputPath = args[1];

			// Add a OneShotBehaviour to send the request to HDFS agent
			addBehaviour(new OneShotBehaviour(this) {
				public void action() {
					
					//searching for an HDFS agent
					DFAgentDescription template = new DFAgentDescription();
					ServiceDescription sd = new ServiceDescription();
					sd.setType(HDFSWriterAgent.SERVICE_HDFS);
					template.addServices(sd);
					try {
						DFAgentDescription[] result = DFService.search(myAgent, template); 
						if(result.length ==0)
						{
							System.out.println("There isn't an HDFSAgent running. Agent terminating."
									+ "Please run the HDFS Agent first");
							doDelete();
						}
						if (result.length>1) {
							System.out.println("There seem to be more HDFS agents running. Agent terminating."
									+ "Please run only one HDFS writer Agent");
							doDelete();
						}
						else if (result.length==1){
			
						hdfsAgent =  result[0].getName();
						System.out.println("Found the following HDFS Agent:" + hdfsAgent.getName());
						//perform the request
						myAgent.addBehaviour(new RequestPerformer());
						}
					}
					catch (FIPAException fe) {
						fe.printStackTrace();
					}

				}
			} );
		}
		else {
			// Make the agent terminate
			System.out.println("Arguments not instantiated correctly."
					+ "Please provide 2 arguments, input path on local "
					+ "filesystem and output path on Hadoop HDFS");
			doDelete();
		}
	}


	protected void takeDown() {
		// Printout a dismissal message
		System.out.println("Sender agent "+getAID().getName()+" terminating.");
	}

	/**
	  * Inner class RequestPerformer.
	  * This is the behaviour used by Sender agent to send the request to HDFS agent 
	 */
	private class RequestPerformer extends Behaviour {
		private MessageTemplate mt; // The template to receive replies
		private int step = 0;

		public void action() {
			switch (step) {
			case 0:
				// Send the cfp to HDFS agent. The HDFS agent will first check if the output file already exists
				// if everything is ok, the copying proceeds
				System.out.println("Sending message to Hadoop Agent to copy file with path " + inputPath +
						" to HDFS output path " + outputPath);
				ACLMessage cfp = new ACLMessage(ACLMessage.CFP);
				cfp.addReceiver(hdfsAgent);
				cfp.setContent(outputPath);
				cfp.setConversationId(HDFSWriterAgent.SERVICE_HDFS);
				cfp.setReplyWith("cfp"+System.currentTimeMillis()); // Unique value
				myAgent.send(cfp);
				// Prepare the template to get reply
				mt = MessageTemplate.and(MessageTemplate.MatchConversationId(HDFSWriterAgent.SERVICE_HDFS),
						MessageTemplate.MatchInReplyTo(cfp.getReplyWith()));
				step = 1;
				break;
			case 1:
				// Receive reply to see if the output path already exists on HDFS
				ACLMessage reply = myAgent.receive(mt);
				if (reply != null) {
					// Reply received
					if (reply.getPerformative() == ACLMessage.PROPOSE) {
						// outputpath doesn't exist, everything ok 
						//String response = reply.getContent();
						step = 2; 
					}
					else if (reply.getPerformative() == ACLMessage.REFUSE)
					{
						System.out.println("process refuse");
						// output path already exists
						System.out.println("Output file path already exists on Hadoop DFS.Please Specify a different path");
						doDelete();
					}

				}
				else {
					block();
				}
				break;
			case 2:
				// Send the input file path to HDFS agent
				ACLMessage proceed = new ACLMessage(ACLMessage.ACCEPT_PROPOSAL);
				proceed.addReceiver(hdfsAgent);
				proceed.setContent(inputPath);
				proceed.setConversationId(HDFSWriterAgent.SERVICE_HDFS);
				proceed.setReplyWith("file"+System.currentTimeMillis());
				myAgent.send(proceed);
				// Prepare the template to get the purchase order reply
				mt = MessageTemplate.and(MessageTemplate.MatchConversationId(HDFSWriterAgent.SERVICE_HDFS),
						MessageTemplate.MatchInReplyTo(proceed.getReplyWith()));
				step = 3;
				break;
			case 3:      
				// Receive reply from HDFS agent
				reply = myAgent.receive(mt);
				if (reply != null) {
					// Purchase order reply received
					if (reply.getPerformative() == ACLMessage.INFORM) {
						// Copy successful. We can terminate
						System.out.println(inputPath+" successfully copied to HDFS "+reply.getSender().getName());
						myAgent.doDelete();
					}
					else {
						System.out.println("Attempt failed: please check output from HDFS Agent");
					}

					step = 4;
				}
				else {
					block();
				}
				break;
			}        
		}

		public boolean done() {
			if (step == 2 && hdfsAgent == null) {
				System.out.println("Attempt failed: There doesn't seem to be an HDFS Agent running currently.");
			}
			return ((step == 2 && hdfsAgent == null) || step == 4);
		}
	}  // End of inner class RequestPerformer
}
