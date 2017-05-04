/**
 * 
 */
package com.didi.etc.its.flowduration;

/**
 * @author didi
 * 2017年2月20日
 */
public class Link  implements java.io.Serializable{

	private static final long serialVersionUID = 11L;
	private String linkid = "";
	private String length = "";
	private String snode = "";
	private String enode ="";
	
	/**
	 * @param linkid
	 * @param length
	 * @param snode
	 * @param enode
	 */
	public Link(String linkid, String length, String snode, String enode) {
		super();
		this.linkid = linkid;
		this.length = length;
		this.snode = snode;
		this.enode = enode;
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
	 * @return the snode
	 */
	public String getSnode() {
		return snode;
	}
	/**
	 * @param snode the snode to set
	 */
	public void setSnode(String snode) {
		this.snode = snode;
	}
	/**
	 * @return the enode
	 */
	public String getEnode() {
		return enode;
	}
	/**
	 * @param enode the enode to set
	 */
	public void setEnode(String enode) {
		this.enode = enode;
	}
}
