/**
 * 
 */
package com.didi.etc.its.flowduration;

import java.util.HashMap;
import java.util.Map;

/**
 * @author didi
 * 2017年2月13日
 */
public class Flow implements java.io.Serializable{
	
	private static final long serialVersionUID = 1L;
	private String junctionid = "";
	private String linkid1 = "";
	private String linkid2 = "";
	private String type = "";
	private String length = "";
	private String linkids = "";
	private String direction = "";
	private String turn = "";
	private String stage = "";
	private Map<String,Link> links = new HashMap<String, Link>();
	
	/**
	 * @param junctionid
	 * @param linkid1
	 * @param linkid2
	 * @param type
	 * @param length
	 * @param linkids
	 * @param direction
	 * @param turn
	 */
	public Flow(String junctionid, String linkid1, String linkid2, String type,
			String length, String linkids, String direction, String turn, String stage, Map<String,Link> links) {
		super();
		this.junctionid = junctionid;
		this.linkid1 = linkid1;
		this.linkid2 = linkid2;
		this.type = type;
		this.length = length;
		this.linkids = linkids;
		this.direction = direction;
		this.turn = turn;
		this.stage = stage;
		this.links = links;
	}
	
	/**
	 * @return the junctionid
	 */
	public String getJunctionid() {
		return junctionid;
	}
	/**
	 * @param junctionid the junctionid to set
	 */
	public void setJunctionid(String junctionid) {
		this.junctionid = junctionid;
	}
	/**
	 * @return the linkid1
	 */
	public String getLinkid1() {
		return linkid1;
	}
	/**
	 * @param linkid1 the linkid1 to set
	 */
	public void setLinkid1(String linkid1) {
		this.linkid1 = linkid1;
	}
	/**
	 * @return the linkid2
	 */
	public String getLinkid2() {
		return linkid2;
	}
	/**
	 * @param linkid2 the linkid2 to set
	 */
	public void setLinkid2(String linkid2) {
		this.linkid2 = linkid2;
	}
	/**
	 * @return the type
	 */
	public String getType() {
		return type;
	}
	/**
	 * @param type the type to set
	 */
	public void setType(String type) {
		this.type = type;
	}
	/**
	 * @return the length
	 */
	public String getLength() {
		return length;
	}
	/**
	 * @param length the length to set
	 */
	public void setLength(String length) {
		this.length = length;
	}
	/**
	 * @return the linkids
	 */
	public String getLinkids() {
		return linkids;
	}
	/**
	 * @param linkids the linkids to set
	 */
	public void setLinkids(String linkids) {
		this.linkids = linkids;
	}
	/**
	 * @return the direction
	 */
	public String getDirection() {
		return direction;
	}
	/**
	 * @param direction the direction to set
	 */
	public void setDirection(String direction) {
		this.direction = direction;
	}
	/**
	 * @return the turn
	 */
	public String getTurn() {
		return turn;
	}
	/**
	 * @param turn the turn to set
	 */
	public void setTurn(String turn) {
		this.turn = turn;
	}

	/**
	 * @return the stage
	 */
	public String getStage() {
		return stage;
	}

	/**
	 * @param stage the stage to set
	 */
	public void setStage(String stage) {
		this.stage = stage;
	}

	/**
	 * @return the links
	 */
	public Map<String, Link> getLinkAttr() {
		return links;
	}

	/**
	 * @param links the links to set
	 */
	public void setLinks(Map<String, Link> links) {
		this.links = links;
	}
}
