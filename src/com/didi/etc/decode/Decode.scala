package com.didi.etc.decode

trait Decode {
  def decode(lines: Iterator[String], pospwd: String,codingFormat:String):Iterator[String]
}