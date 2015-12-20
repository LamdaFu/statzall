package statzall.cdap;

import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.OutputEmitter;
import co.cask.cdap.api.flow.flowlet.StreamEvent;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Charsets;

public class StreamStatParseFlowlet extends AbstractFlowlet {
	static final String NAME = "streamStatParse";

	private OutputEmitter<String[]> out;

	@ProcessInput
	public void process(StreamEvent diskMetrics) {
		String event = Charsets.UTF_8.decode(diskMetrics.getBody()).toString();
		String[] fields = event.split(",", 2);
		if(StringUtils.isEmpty(fields[0]) || fields.length < 2) {
			return;
		}
		// Trunc to sane length for stat calculation
		for (int x = 0; x < fields.length; x++) {
			if (fields[x].length() > 64) {
				fields[x] = fields[x].substring(0, 64);
			}
		}
		out.emit(fields, "streamStatId", fields[0]);
	}
}