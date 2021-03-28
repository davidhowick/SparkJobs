package com.github.io

import java.io.{File, PrintWriter}


class FlatFileWriter  (PathName: String) {
  val pw = new PrintWriter(new File(PathName ))

  def writeListToFile(list: Array[String]) {
    list.foreach(product => pw.write(product + "\n"))
    pw.close
  }

  def writeValueToFile(value: String) {
    pw.write(value)
    pw.close
  }

  def writeMapToFile(map: Array[(String, Int)]){
    map.foreach(product => pw.write(product + "\n"))
    pw.close
  }

  def writeRowsToFile(rows: Unit): Unit ={
    pw.write(rows + "\n")
    pw.close
  }
}
