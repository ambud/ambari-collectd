/**
 * Copyright 2017 Ambud Sharma
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * 		http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.srotya.collectd.ambari;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.collectd.api.Collectd;
import org.collectd.api.CollectdConfigInterface;
import org.collectd.api.CollectdReadInterface;
import org.collectd.api.CollectdShutdownInterface;
import org.collectd.api.OConfigItem;
import org.collectd.api.OConfigValue;
import org.collectd.api.PluginData;
import org.collectd.api.ValueList;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

/**
 * @author ambud
 */
public class AmbariMetrics implements CollectdConfigInterface, CollectdReadInterface, CollectdShutdownInterface {

	private static final String AMBARI = "Ambari";
	private static final String URL = "http://{{ambari_collector_host}}:{{ambari_collector_port}}/ws/v1/timeline/metrics?metricNames={{metric_name}}&hostname={{hostname}}&appId={{appId}}&precision=seconds&startTime={{start_time}}&endTime={{end_time}}";
	private AmbariInstance instance;
	private List<AppMetrics> appMetrics;
	private HttpClientBuilder builder;
	private long ts;

	public AmbariMetrics() {
		appMetrics = new ArrayList<>();
		Collectd.registerConfig(AMBARI, this);
		Collectd.registerRead(AMBARI, this);
		Collectd.registerShutdown(AMBARI, this);
	}

	@Override
	public int shutdown() {
		return 0;
	}

	@Override
	public int read() {
		String baseUrl = URL.replace("{{ambari_collector_host}}", instance.getAmbariMetricsHostname());
		baseUrl = baseUrl.replace("{{ambari_collector_port}}", String.valueOf(instance.getAmbariMetricsPort()));
		for (AppMetrics appMetric : appMetrics) {
			PluginData pd = new PluginData();
			pd.setPlugin(appMetric.getAppId());
			baseUrl = baseUrl.replace("{{appId}}", appMetric.appId);
			if (appMetric.getHostnames().size() == 0) {
				continue;
			}
			if (appMetric.getMetricNames().size() == 0) {
				continue;
			}
			String hostnameString = toURLHostnames(appMetric);
			baseUrl = baseUrl.replace("{{hostname}}", hostnameString);

			String metricNames = toURLMetricnames(appMetric);
			baseUrl = baseUrl.replace("{{metric_name}}", metricNames);

			baseUrl = baseUrl.replace("{{start_time}}", String.valueOf(ts));
			baseUrl = baseUrl.replace("{{end_time}}", String.valueOf(System.currentTimeMillis()));

			HttpGet request = new HttpGet(baseUrl);
			CloseableHttpClient client = builder.build();
			try {
				CloseableHttpResponse response = client.execute(request);
				String result = EntityUtils.toString(response.getEntity());

				JsonObject object = new Gson().fromJson(result, JsonObject.class);
				for (JsonElement element : object.get("metrics").getAsJsonArray()) {
					JsonObject metric = element.getAsJsonObject();
					pd.setHost(metric.get("hostname").getAsString());
					ValueList values = new ValueList(pd);
					values.setType("records");
					values.setTypeInstance(metric.get("metricname").getAsString());

					JsonObject metrics = metric.get("metrics").getAsJsonObject();
					for (Entry<String, JsonElement> entry : metrics.entrySet()) {
						values.setValues(Arrays.asList(entry.getValue().getAsNumber()));
						values.setTime(Long.parseLong(entry.getKey()));
						Collectd.dispatchValues(values);
					}
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
		ts = System.currentTimeMillis() - 60_000;
		return 0;
	}

	public String toURLMetricnames(AppMetrics appMetric) {
		StringBuilder tempMetricNames = new StringBuilder();
		tempMetricNames.append(appMetric.getMetricNames().get(0));
		for (int i = 1; i < appMetric.getMetricNames().size(); i++) {
			String tempMetricName = appMetric.getMetricNames().get(i);
			tempMetricNames.append("," + tempMetricName);
		}
		return tempMetricNames.toString();
	}

	public String toURLHostnames(AppMetrics appMetric) {
		StringBuilder tempHostnames = new StringBuilder();
		tempHostnames.append(appMetric.getHostnames().get(0));
		for (int i = 1; i < appMetric.getHostnames().size(); i++) {
			String tempHostname = appMetric.getHostnames().get(i);
			tempHostnames.append("," + tempHostname);
		}
		return tempHostnames.toString();
	}

	@Override
	public int config(OConfigItem config) {
		try {
			instance = new AmbariInstance();
			for (OConfigItem ambariInstance : config.getChildren()) {
				if (ambariInstance.getKey().equalsIgnoreCase("hostname")) {
					instance.setAmbariMetricsHostname(ambariInstance.getValues().get(0).getString());
				}
				if (ambariInstance.getKey().equalsIgnoreCase("port")) {
					instance.setAmbariMetricsPort(ambariInstance.getValues().get(0).getNumber().intValue());
				}
				if (ambariInstance.getKey().equalsIgnoreCase("app")) {
					AppMetrics app = new AppMetrics();
					for (OConfigItem appItem : ambariInstance.getChildren()) {
						if (appItem.getKey().equalsIgnoreCase("appid")) {
							app.setAppId(appItem.getValues().get(0).getString());
						}
						if (appItem.getKey().equalsIgnoreCase("hostnames")) {
							for (OConfigValue tmp : appItem.getValues()) {
								app.getHostnames().add(tmp.getString());
							}
						}
						if (appItem.getKey().equalsIgnoreCase("metricnames")) {
							for (OConfigValue tmp : appItem.getValues()) {
								app.getMetricNames().add(tmp.getString());
							}
						}
					}
					appMetrics.add(app);
				}
			}
			builder = HttpClientBuilder.create();
		} catch (Exception e) {
			e.printStackTrace();
			Collectd.logError("Initialization failed:" + e.getMessage());
			return -1;
		}
		if (instance == null || instance.getAmbariMetricsHostname() == null) {
			return -1;
		}
		ts = System.currentTimeMillis() - 60_000;
		return 0;
	}

	public static class AppMetrics {

		private List<String> metricNames;
		private List<String> hostnames;
		private String appId;

		public AppMetrics() {
			metricNames = new ArrayList<>();
			hostnames = new ArrayList<>();
		}

		/**
		 * @return the metricNames
		 */
		public List<String> getMetricNames() {
			return metricNames;
		}

		/**
		 * @return the hostnames
		 */
		public List<String> getHostnames() {
			return hostnames;
		}

		/**
		 * @return the appId
		 */
		public String getAppId() {
			return appId;
		}

		/**
		 * @param appId
		 *            the appId to set
		 */
		public void setAppId(String appId) {
			this.appId = appId;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.lang.Object#toString()
		 */
		@Override
		public String toString() {
			return "AppMetrics [metricNames=" + metricNames + ", hostnames=" + hostnames + ", appId=" + appId + "]";
		}

	}

	public static class AmbariInstance {

		private String ambariMetricsHostname = "localhost";
		private int ambariMetricsPort = 6188;

		public AmbariInstance() {
		}

		/**
		 * @return the ambariMetricsHostname
		 */
		public String getAmbariMetricsHostname() {
			return ambariMetricsHostname;
		}

		/**
		 * @param ambariMetricsHostname
		 *            the ambariMetricsHostname to set
		 */
		public void setAmbariMetricsHostname(String ambariMetricsHostname) {
			this.ambariMetricsHostname = ambariMetricsHostname;
		}

		/**
		 * @return the ambariMetricsPort
		 */
		public int getAmbariMetricsPort() {
			return ambariMetricsPort;
		}

		/**
		 * @param ambariMetricsPort
		 *            the ambariMetricsPort to set
		 */
		public void setAmbariMetricsPort(int ambariMetricsPort) {
			this.ambariMetricsPort = ambariMetricsPort;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.lang.Object#toString()
		 */
		@Override
		public String toString() {
			return "AmbariInstance [ambariMetricsHostname=" + ambariMetricsHostname + ", ambariMetricsPort="
					+ ambariMetricsPort + "]";
		}
	}
}
