package statzall.cdap;

import co.cask.cdap.api.flow.AbstractFlow;

public class StreamStatFlow extends AbstractFlow {
	static final String NAME = "StreamStatFlow";

	@Override
	protected void configureFlow() {
		setName(NAME);
		setDescription("Calculates streaming stats");
		addFlowlet(StreamStatParseFlowlet.NAME, new StreamStatParseFlowlet());
		addFlowlet(StreamStatCalcFlowlet.NAME, new StreamStatCalcFlowlet());
		connectStream(StreamStatApp.STREAM_NAME, StreamStatParseFlowlet.NAME);
		connect(StreamStatParseFlowlet.NAME, StreamStatCalcFlowlet.NAME);
	}

}