package com.didi.etc.hdfs.tool

import scala.io.Source

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.fs.FSDataOutputStream
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import scala.collection.mutable.Set
import scala.beans.BeanProperty
import com.didi.etc.its.common.util.DataFormatUtil

/**
 * @author 宿荣全
 * 合并小文件，删除小文件工具
 */
class HDFSUtil {

  var HDFSFileSytem: FileSystem = null
  val pathSet = Set[Path]()
  
  /**
   * 获取hdfs
   */
  def HDFSInit = (HDFSFileSytem = FileSystem.get( new Configuration))
  
  /**
   * 遍历正个filePathSet的每一个path，然后进行合并处理
   */
  def unionAllFile(filePathSet: Set[Path]) = filePathSet.foreach { srcPath => mergeFile(srcPath) }

  /**
   * 遍历正个filePathSet的每一个path，然后进行删除处理
   * del_flg＝true: 删除文件名跟filterRegex匹配的文件
   * del_flg＝false: 不删除文件名跟filterRegex匹配的文件
   */
  def delAllFile(filePathSet: Set[Path], filterRegex: String, del_flg: Boolean) =
    filePathSet.foreach { srcPath => deleSubFile(srcPath, filterRegex, del_flg) }

  /**
   * .merge 是已经合并过的文件，.writting是正在生成的文件
   */
  def defalutFilter(filename: String) = !(filename).endsWith(".merge") && !(filename).endsWith(".writting")
  /**
   * 合并本目录下的所有文件，生成新文件date.merge
   */
  def mergeFile(srcPath: Path) = {
    if (isFileExists(srcPath)) {
      val listStatus = HDFSFileSytem.listStatus(srcPath)
      val list = listStatus.map { file => file.getPath }.filter { filepath => defalutFilter(filepath.getName) }
      val separator = System.getProperty("line.separator")
      if (list.size > 0) {
        val desName = srcPath.toString + "/" + DataFormatUtil.df1.format(System.currentTimeMillis) + ".merge"
        val outputDFS = HDFSFileSytem.create(new Path(desName))

        list.foreach { filepath =>
          {
            val srcDFSInput = HDFSFileSytem.open(filepath)
            // 过滤掉字段个数不复的数据,返回以拆分关键字为key,line为结果的数据集（true/false,key,line ）
            val lines = Source.fromInputStream(srcDFSInput, "UTF-8").getLines
            
            outputDFS.writeBytes(lines.mkString(separator))
            outputDFS.flush()
//            lines.foreach { line =>
//              {
//                outputDFS.writeBytes(line)
//                outputDFS.writeBytes(separator)
//                outputDFS.flush()
//              }
//            }
            srcDFSInput.close
            HDFSFileSytem.delete(filepath, false)
            println("Hdfs文件合并完成并移除：" + filepath.toString)
          }
        }
        outputDFS.close()
        println("目录:" + srcPath.toString + "的全部文件合并到：" + desName + "完成！")
      }
    }
  }

  /**
   * 判断此文件或目录在ＨＤＦＳ上是否存在
   */
  def isFileExists(path: Path) = HDFSFileSytem.isDirectory(path)

  /**
   * 递归遍历要合并的所有子目录
   */
  def pathList(path: Path) {
    if (HDFSFileSytem.isDirectory(path)) {
      val listStatus = HDFSFileSytem.listStatus(path).map { fileInfo => fileInfo.getPath }
      var flg = false
      listStatus.foreach { subpath =>
        {
          if (HDFSFileSytem.isDirectory(subpath)) pathList(subpath) else {
            if (!flg) {
              pathSet += path
              flg = true
            }
          }
        }
      }
    }
  }

  /**
   * 递归遍历要合并的所有子目录下的所有文件
   */
  def deleSubFile(path: Path, filterRegex: String, del_flg: Boolean) {
    if (HDFSFileSytem.isDirectory(path)) {
      val listStatus = HDFSFileSytem.listStatus(path).map { fileInfo => fileInfo.getPath }
      listStatus.foreach { subpath =>
        if (HDFSFileSytem.isDirectory(subpath)) pathList(subpath) else {
          if (isDeleteFile(subpath.getName, filterRegex, del_flg)) {
            delteFile(subpath)
            println("删除文件：" + subpath)
          }
        }
      }
    } else if (isDeleteFile(path.getName, filterRegex, del_flg)) {
      delteFile(path)
      println("删除文件：" + path)
    }
  }

  //＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝
  /**
   * 删除hdfs文件,true:删除文件夹及其下文件
   */
  def delteFile(filePath: Path) = HDFSFileSytem.delete(filePath, true)

  /**
   * 文件名是否用匹配户指定的文件规则
   */
  def isMatch(fileName: String, filterRegex: String) = fileName.matches(filterRegex)
  /**
   * 按规则判断是否是属于要删除的文件
   */
  def isDeleteFile(fileName: String, filterRegex: String, del_flg: Boolean) = {
    // 默认规则：.merge .writting这两类是系统生成的
    if (defalutFilter(fileName)) {
      if (del_flg) {
        if (isMatch(fileName, filterRegex)) true else false
      } else {
        if (isMatch(fileName, filterRegex)) false else true
      }
    } else false

  }
  
    /**
   * 读取hdfs文件
   */
  def getHdfsFileLines(inputStream: FSDataInputStream) = Source.fromInputStream(inputStream, "UTF-8").getLines.toList
}