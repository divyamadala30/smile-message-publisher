package org.mskcc.cmo.publisher.pipeline.limsrest;

import com.google.gson.Gson;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
import org.mskcc.cmo.messaging.Gateway;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

/**
 *
 * @author ochoaa
 */
public class LimsRequestWriter implements ItemStreamWriter<Map<String, Object>> {

    @Autowired
    private Gateway messagingGateway;

    @Value("${igo.new_request_topic}")
    private String IGO_NEW_REQUEST_TOPIC;

    private final Logger LOG = Logger.getLogger(LimsRequestWriter.class);

    @Override
    public void open(ExecutionContext ec) throws ItemStreamException {
        try {
            messagingGateway.connect();
        } catch (Exception ex) {
            throw new RuntimeException("Error occurred during attempt to connect to messaging gateway", ex);
        }
    }

    @Override
    public void update(ExecutionContext ec) throws ItemStreamException {}

    @Override
    public void close() throws ItemStreamException {}

    @Override
    public void write(List<? extends Map<String, Object>> requestResponseList) throws Exception {
        for (Map<String, Object> request : requestResponseList) {
            Gson gson = new Gson();
            try {
                String requestJson = gson.toJson(request);
                LOG.debug("\nPublishing IGO new request to MetaDB:\n\n"
                        + requestJson + "\n\n on topic: " + IGO_NEW_REQUEST_TOPIC);
                messagingGateway.publish(IGO_NEW_REQUEST_TOPIC, requestJson);
            } catch (Exception e) {
                LOG.error("Error encountered during attempt to process request ids - exiting...");
                throw new RuntimeException(e);
            }
        }
    }

}
