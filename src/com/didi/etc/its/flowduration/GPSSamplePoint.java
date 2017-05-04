/**
 * 
 */
package com.didi.etc.its.flowduration;

/**
 * @author didi
 * 2017年2月13日
 */
public class GPSSamplePoint {
	private String timestamp = "";
	private String lng = "";
	private String lat = "";
	private String linkid = "";
	private String linkDist = "";
	/**
	 * @param timestamp
	 * @param lng
	 * @param lat
	 * @param linkid
	 * @param linkDist
	 */
	public GPSSamplePoint(String timestamp, String lng, String lat,
			String linkid, String linkDist) {
		super();
		this.timestamp = timestamp;
		this.lng = lng;
		this.lat = lat;
		this.linkid = linkid;
		this.linkDist = linkDist;
	}
	/**
	 * @return the timestamp
	 */
	public String getTimestamp() {
		return timestamp;
	}
	/**
	 * @param timestamp the timestamp to set
	 */
	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}
	/**
	 * @return the lng
	 */
	public String getLng() {
		return lng;
	}
	/**
	 * @param lng the lng to set
	 */
	public void setLng(String lng) {
		this.lng = lng;
	}
	/**
	 * @return the lat
	 */
	public String getLat() {
		return lat;
	}
	/**
	 * @param lat the lat to set
	 */
	public void setLat(String lat) {
		this.lat = lat;
	}
	/**
	 * @return the linkid
	 */
	public String getLinkid() {
		return linkid;
	}
	/**
	 * @param linkid the linkid to set
	 */
	public void setLinkid(String linkid) {
		this.linkid = linkid;
	}
	/**
	 * @return the linkDist
	 */
	public String getLinkDist() {
		return linkDist;
	}
	/**
	 * @param linkDist the linkDist to set
	 */
	public void setLinkDist(String linkDist) {
		this.linkDist = linkDist;
	}
}
