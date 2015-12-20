package statzall.cdap;

import static org.junit.Assert.*;

import co.cask.cdap.api.metrics.RuntimeMetrics;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.StreamManager;
import co.cask.cdap.test.TestBase;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import statzall.cdap.StreamStatApp;
import statzall.cdap.StreamStatCalcFlowlet;
import statzall.cdap.StreamStatFlow;
import statzall.cdap.StreamStatHttpHandler;
import statzall.cdap.StreamStatParseFlowlet;

import com.google.common.base.Charsets;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import cern.jet.random.Normal;
import cern.jet.random.engine.MersenneTwister;

import org.junit.Assert;
import org.junit.Test;

import java.net.URL;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class StreamStatAppTest extends TestBase {
	private static final Gson GSON = new Gson();

	@Test
	public void test() throws Exception {
		// Deploy the application
		ApplicationManager appManager = deployApplication(StreamStatApp.class);

		// Start the flow
		FlowManager flowManager = appManager.getFlowManager(StreamStatFlow.NAME);
		flowManager.start();

		int n = 256;
		long start = 0L;
		double secs = 0D;
		try {
			StreamManager streamManager = getStreamManager(StreamStatApp.STREAM_NAME);

			Normal norm = new Normal(10000, 50, new MersenneTwister());
			start = System.currentTimeMillis();
			for (int x = 0; x < n; x++) {
				streamManager.send(String.format("key%s,%s", x % 4, norm.nextInt()));
			}

			RuntimeMetrics countMetrics = flowManager.getFlowletMetrics(StreamStatParseFlowlet.NAME);
			countMetrics.waitForProcessed(n, 100, TimeUnit.SECONDS);
			countMetrics = flowManager.getFlowletMetrics(StreamStatCalcFlowlet.NAME);
			countMetrics.waitForProcessed(n, 100, TimeUnit.SECONDS);
			secs = (System.currentTimeMillis() - start) / 1000D;
						
			// Start service and verify
			ServiceManager serviceManager = appManager.getServiceManager(StreamStatHttpHandler.NAME);
			serviceManager.start();
			serviceManager.waitForStatus(true);
			try {
				URL serviceUrl = serviceManager.getServiceURL();

				URL url = new URL(serviceUrl, "stats/all");
				HttpRequest request = HttpRequest.get(url).build();
				HttpResponse response = HttpRequests.execute(request);
				Assert.assertEquals(200, response.getResponseCode());
				Map<String, Map<String, Object>> stats = GSON.fromJson(response.getResponseBodyAsString(Charsets.UTF_8),
						new TypeToken<Map<String, Map<String, Object>>>() {
						}.getType());
				Assert.assertEquals(4, stats.size());
				Assert.assertTrue(stats.containsKey("key1"));
			} finally {
				serviceManager.stop();
				serviceManager.waitForStatus(false);
			}
		} finally {
			flowManager.stop();
		}
		System.out.println(String.format("\n\n\n%s count, %.3f secs, %.3f count/sec", n, secs, n / secs));
	}
}
