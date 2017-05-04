package com.didi.etc.its.flowduration;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;

import com.didi.util.auto.GPS;

/**
 * @author 宿荣全.滴滴智能交通云
 * @data 2017年3月7日 下午9:38:25 Description:
 *       <p>
 *       <／p>
 * @version 1.0
 * @since JDK 1.7.0_80
 */
public class FlowUtil {

	public static HashMap<String, Flow> getFlowFromFile(InputStream in) {
		HashMap<String, Flow> flows = new HashMap<String, Flow>();
		try {
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String line = br.readLine();
			String flow[] = null;
			while (line != null) {
				flow = line.split("\t");
				if (flow.length != 11) {
					System.out.println("illegal flow record, the length is "
							+ flow.length);
				} else if (flow[3].equals("true") || flow[3].equals("1")) {
					flows.put(flow[1] + "@" + flow[2],
							new Flow(flow[0], flow[1], flow[2], flow[3],
									flow[4], flow[5].replaceAll("[|]", ""),
									flow[7], flow[8], flow[9],
									getLinkAttrs(flow[10])));
				}
				line = br.readLine();
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return flows;
	}

	public static Map<String, Link> getLinkAttrs(String attrString) {
		Map<String, Link> links = new HashMap<String, Link>();
		try {
			if (!attrString.equals("null")) {
				JSONArray array = new JSONArray(attrString);
				for (int i = 0; i < array.length(); i++) {
					JSONObject jo = (JSONObject) array.get(i);
					links.put("" + jo.getInt("linkid"),
							new Link("" + jo.get("linkid"), "" + jo.get("len"),
									"" + jo.get("snode"), "" + jo.get("enode")));
				}
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return links;
	}

	public static List<String> mapFuction(String junctionList,
			int timeDuration, String value, HashMap<String, Flow> FLOWS) {
		List<String> result = new ArrayList<String>();
		List<String> junctionids = Arrays.asList(junctionList.split(","));
		try {
			Calendar calendar = Calendar.getInstance();
			String[] record = value.split(" |\t");
			String tmp[] = null;
			if (record.length == 3) {
				ArrayList<GPSSamplePoint> orbit = new ArrayList<GPSSamplePoint>();
				for (String point : record[2].split(";")) {
					tmp = point.split(",");
					if (tmp.length == 10) {
						// tmp[1] timestamp tmp[3] linkid tmp[4]&tmp[5] gps
						for (String s : tmp[3].split("\\|")) {
							orbit.add(new GPSSamplePoint(tmp[0], tmp[1],
									tmp[2], s, tmp[4]));
						}
					} else {
						System.out
								.println("illegel orbit sample point, the length is "
										+ tmp.length);
					}
				}
				int counter = orbit.size();
				long duration = 0L;
				int lastI = 0;
				for (int i = 0; i < counter; i++) {
					// 若是连续时间内经过同一个起点link，只用第一个取样点的时间
					double distance = 0;
					if (i + 1 <= counter - 1
							&& !orbit.get(i).getLinkid()
									.equals(orbit.get(i + 1).getLinkid())) {
						for (int j = i + 1; j < counter; j++) {
							// 该起点link第二次出现，后续的外循环会进行处理，此时退出；或者当前点与起始点的gps距离超过flow的length，也退出
							if (orbit.get(lastI).getLinkid()
									.equals(orbit.get(j).getLinkid())) {
								break;
							}
							double tmpDis = GPS.GetDistance(Double
									.parseDouble(orbit.get(lastI).getLng()),
									Double.parseDouble(orbit.get(lastI)
											.getLat()),
									Double.parseDouble(orbit.get(j).getLng()),
									Double.parseDouble(orbit.get(j).getLat()));
							if (tmpDis > distance) {
								distance = tmpDis;
							}
							// 匹配到目标需要计算的flow
							if (FLOWS.containsKey(orbit.get(lastI).getLinkid()
									+ "@" + orbit.get(j).getLinkid())) {
								// 0-(linkid1_linkid2),1-junction2,2-length,3-stage
								Flow flowInfo = FLOWS.get(orbit.get(lastI)
										.getLinkid()
										+ "@"
										+ orbit.get(j).getLinkid());
								if ((distance * 1000) < Double
										.parseDouble(flowInfo.getLength())) {
									// 若是连续时间经过同一个终点link，只用第一个取样点
									duration = Long.parseLong(orbit.get(j)
											.getTimestamp())
											- Long.parseLong(orbit.get(lastI)
													.getTimestamp());
									if (duration < 1200) {
										/**
										 * Key: 路口 + 时间区间,Value: 阶段 + flowid +
										 * Duration
										 */
										long interval = Long.parseLong(orbit
												.get(lastI).getTimestamp())
												- (Long.parseLong(orbit.get(
														lastI).getTimestamp()) % timeDuration);
										calendar.setTimeInMillis(interval * 1000);
										String hour = String
												.format("%02d",
														calendar.get(Calendar.HOUR_OF_DAY));
										String min = String.format("%02d",
												calendar.get(Calendar.MINUTE));

										String times = (new java.text.SimpleDateFormat(
												"yyyyMMdd")).format(calendar
												.getTime());
										if (junctionids.contains(flowInfo
												.getJunctionid()) && flowInfo.getTurn().equals(0) || flowInfo.getTurn().equals(2)) {
											result.add(times + "\t"
													+ flowInfo.getJunctionid()
													+ "\t" + hour + ":" + min
													+ "\t"
													+ flowInfo.getLinkid1()
													+ "\t"
													+ flowInfo.getLinkid2()
													+ "\t"
													+ flowInfo.getDirection()
													+ "\t" + flowInfo.getTurn()
													+ "\t" + duration);
										}
									}
									break;
								}
							}
						}
						lastI = i + 1;
					}
				}
			} else {
				System.out.println("illegal driver record, the length is"
						+ record.length);
				System.out.println(value.toString());
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return result;
	}
}