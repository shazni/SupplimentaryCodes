package org.wso2.carbon.registry.samples.receiver;
  
import org.apache.axiom.om.OMElement;
import org.apache.axiom.om.xpath.AXIOMXPath;
import org.apache.axiom.soap.SOAPEnvelope;
import org.apache.axis2.AxisFault;
import org.apache.axis2.context.MessageContext;
import org.apache.axis2.receivers.AbstractMessageReceiver;
import org.wso2.carbon.databridge.agent.thrift.Agent;
import org.wso2.carbon.databridge.agent.thrift.DataPublisher;
import org.wso2.carbon.databridge.agent.thrift.conf.AgentConfiguration;
import org.wso2.carbon.databridge.commons.Event;
import org.wso2.carbon.databridge.commons.exception.NoStreamDefinitionExistException;
import org.wso2.carbon.registry.core.utils.RegistryUtils;
import org.wso2.carbon.utils.NetworkUtils;
  
import java.util.ArrayList;
  
public class SampleMessageReceiver extends AbstractMessageReceiver {
  
    public static final String NAMESPACE = "http://wso2.org/ns/2011/01/eventing/registry/event";
    public static final String REGISTRY_ACTIVITY_STREAM = "org.wso2.bam.registry.activity.kpi";
    public static final String VERSION = "1.0.0";
  
    protected void invokeBusinessLogic(MessageContext messageContext) throws AxisFault {
        SOAPEnvelope envelope = messageContext.getEnvelope();
        try {
	    // Find Username and Operation
            AXIOMXPath xPath = new AXIOMXPath("//ns:RegistryOperation");
            xPath.addNamespace("ns", NAMESPACE);
            String operation = ((OMElement)((ArrayList)xPath.evaluate(envelope)).get(0)).getText();
            xPath = new AXIOMXPath("//ns:Username");
            xPath.addNamespace("ns", NAMESPACE);
            String username = ((OMElement)((ArrayList)xPath.evaluate(envelope)).get(0)).getText();

	    // Create Data Publisher
            RegistryUtils.setTrustStoreSystemProperties();
            DataPublisher dataPublisher = new DataPublisher(
                    "tcp://" + NetworkUtils.getLocalHostname() + ":7612", "admin", "admin",
                    new Agent(new AgentConfiguration()));

	    // Find Data Stream
            String streamId;
            try {
                streamId = dataPublisher.findStream(REGISTRY_ACTIVITY_STREAM, VERSION);
            } catch (NoStreamDefinitionExistException ignored) {
                streamId = dataPublisher.defineStream("{" +
                        "  'name':'" + REGISTRY_ACTIVITY_STREAM + "'," +
                        "  'version':'" + VERSION + "'," +
                        "  'nickName': 'Registry_Activity'," +
                        "  'description': 'Registry Activities'," +
                        "  'metaData':[" +
                        "          {'name':'clientType','type':'STRING'}" +
                        "  ]," +
                        "  'payloadData':[" +
                        "          {'name':'operation','type':'STRING'}," +
                        "          {'name':'user','type':'STRING'}" +
                        "  ]" +
                        "}");
            }

	    if (!streamId.isEmpty()) {
		// Publish Event to Stream
                dataPublisher.publish(new Event(
                        streamId, System.currentTimeMillis(),
                        new Object[]{"external"}, null, new Object[]{
                        operation, username}));
                dataPublisher.stop();
                System.out.println("Successfully Published Event");
            }
  
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
