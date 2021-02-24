package org.mskcc.cmo.publisher.pipeline.limsrest;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;

/**
 *
 * @author ochoaa
 */
public class LimsRequestProcessor implements ItemProcessor<String, Map<String, Object>> {
    @Autowired
    private LimsRequestUtil limsRestUtil;

    @Override
    public Map<String, Object> process(String requestId) throws Exception {
        CompletableFuture<Map<String, Object>> futureRequestResponse =
                limsRestUtil.getLimsRequestSamples(requestId);
        Map<String, Object> requestResponse = futureRequestResponse.get();
        List<String> sampleIds = limsRestUtil.getSampleIdsFromRequestResponse(requestResponse);

        // get sample manifest for each sample id
        List<Object> sampleManifestList = new ArrayList<>();
        for (String sampleId : sampleIds) {
            CompletableFuture<Object> manifest = limsRestUtil.getSampleManifest(sampleId);
            if (manifest != null) {
                sampleManifestList.add(manifest.get());
            } else {
                limsRestUtil.updateLimsRequestErrors(requestId, sampleId);
            }
        }

        // update request response with sample manifests fetched
        // and add project id as well for cmo metadb
        String projectId = requestId.split("_")[0];
        requestResponse.put("projectId", projectId);
        requestResponse.put("sampleManifestList", sampleManifestList);
        return requestResponse;
    }

}
