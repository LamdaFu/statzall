package statzall.cdap;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.lib.KeyValueTable;

@SuppressWarnings("rawtypes")
public class StreamStatApp extends AbstractApplication {
	static final String APP_NAME = "StreamStatApp";
	static final String STREAM_NAME = "statStream";

	@SuppressWarnings("unchecked")
	@Override
	public void configure() {
		setName(APP_NAME);
		Stream stream = new Stream(STREAM_NAME);
		addStream(stream);
		createDataset("streamStat", KeyValueTable.class);
		addFlow(new StreamStatFlow());
		addService(StreamStatHttpHandler.NAME, new StreamStatHttpHandler());
	}
}