package sae.analyses.profiler.util

/**
 * @author Mirko Köhler
 */
trait DatabaseReader {

  def addArchive (stream : java.io.InputStream)

  def addClassFile (stream : java.io.InputStream)
  def removeClassFile (stream : java.io.InputStream)
  def updateClassFile (oldStream : java.io.InputStream, newStream : java.io.InputStream)

  def classCount : Int
  def methodCount : Int
  def fieldCount : Int
  def instructionCount : Int

}
