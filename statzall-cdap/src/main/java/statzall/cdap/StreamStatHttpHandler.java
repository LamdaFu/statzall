package statzall.cdap;

import java.util.Iterator;
import java.util.Map;

import javax.ws.rs.GET;
import javax.ws.rs.Path;

import org.apache.commons.collections4.trie.PatriciaTrie;

import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceContext;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import statzall.codec.StreamCalcKryo;

public class StreamStatHttpHandler extends AbstractHttpServiceHandler {
	static final String NAME = "StreamStatService";

	@UseDataSet("streamStat")
	private KeyValueTable streamStat;
	private StreamCalcKryo ser;

	@Override
	public void initialize(HttpServiceContext context) throws Exception {
		ser = new StreamCalcKryo();
	}

	@Path("stats/all")
	@GET
	public void getAllStats(HttpServiceRequest request, HttpServiceResponder responder) {
		Iterator<KeyValue<byte[], byte[]>> streamStatScan = streamStat.scan(null, null);
		PatriciaTrie<Map<String, Object>> results = new PatriciaTrie<>();
		while (streamStatScan.hasNext()) {
			KeyValue<byte[], byte[]> result = streamStatScan.next();
			results.put(new String(result.getKey()), ser.read(result.getValue()).snapshot());
		}
		responder.sendJson(200, results);
	}
}