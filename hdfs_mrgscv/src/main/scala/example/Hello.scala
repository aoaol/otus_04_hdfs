package example

import java.io.{ BufferedReader, InputStreamReader, PrintWriter }
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import java.net.URI
import org.apache.log4j.BasicConfigurator

object Hello extends Greeting with App {
  val outDir = "/ods"
  val inDir = "/stage"
  val dataFileExtension = ".csv"
  val tempDestConcatName = "temp_concat_csv.tmp"
  BasicConfigurator.configure()

  prn( greeting )

  val conf = new Configuration()
  // Enabling append().
  conf.setBoolean("dfs.support.append", true);
  conf.setBoolean("dfs.client.block.write.replace-datanode-on-failure.enable", false);
  conf.set("fs.client.block.write.replace-datanode-on-failure.policy","NEVER")

  val fs = FileSystem.get( new URI("hdfs://localhost:9000"), conf)

  val outPath = new Path( outDir )
  if (! fs.exists( outPath))
      fs.mkdirs( outPath )

  prn(s" Folder $inDir")
  for( inSubDir <- fs.listStatus( new Path( inDir )) if inSubDir.isDirectory) {
    prn( s"subDir: ${inSubDir.getPath.getName}")
    val outSubDirName = s"$outDir/${inSubDir.getPath.getName}"
    val outSubDirNameD = outSubDirName + "/"
    val outSubDirPath = new Path( outSubDirName )

    if (! fs.exists( outSubDirPath ))
        fs.mkdirs( outSubDirPath )

    val inSubDirName = s"$inDir/${inSubDir.getPath.getName}"
    val inSubDirNameD = inSubDirName + "/"
    val inFilesStatus = fs.listStatus( new Path( inSubDirName ))

    // Deleting new empty data files.
    for( inFile <- inFilesStatus   if inFile.isFile   &&   inFile.getLen == 0   &&   inFile.getPath.getName.endsWith( dataFileExtension ))
      fs.delete( new Path( inSubDirNameD + inFile.getPath.getName), false)

    // Collecting new data files
    var inFiles = for( inFile <- inFilesStatus    if inFile.isFile   &&   inFile.getLen > 0   &&   inFile.getPath.getName.endsWith( dataFileExtension ))
                  yield new Path( outSubDirNameD + inFile.getPath.getName )

    if (inFiles.nonEmpty) {
      inFiles.foreach( x => prn( s"   src file:  ${x.getName}" ))

      val tempDestFilePath = new Path( outSubDirNameD + tempDestConcatName )
      val destFiles = for( destFile <- fs.listStatus( new Path( outSubDirName ))   if destFile.isFile)    yield new Path( outSubDirNameD + destFile.getPath.getName )
      val destFilePath = new Path( outSubDirNameD + (if (destFiles.nonEmpty)  destFiles(0).getName else inFiles(0).getName) )
      prn( s"   dest file: ${destFilePath.getName}")

      if (destFiles.nonEmpty) {
        // An accumulator file (in ods/...) exists.
        // Renaming the accumulator file to avoid name collision with new files.

        fs.rename( destFilePath, tempDestFilePath)
        inFiles.foreach( inf => fs.rename( new Path( inSubDirNameD + inf.getName ), inf ))
        inFiles.foreach( inf => appendStringToFile( inf, "\n"))
      }
      else {
        // Accumulator file does not exist - using the first new data file as an accumulator.
        inFiles.foreach( inf => fs.rename( new Path( inSubDirNameD + inf.getName ), inf ))
        inFiles.foreach( inf => appendStringToFile( inf, "\n"))
        fs.rename( destFilePath, tempDestFilePath)
        inFiles = inFiles.drop( 1 )
      }
      prn( s"   after rename & append inFiles")

      fs.concat( tempDestFilePath, inFiles)

      fs.rename( tempDestFilePath, destFilePath )
    }
    else
      println("--->   NO data files to merge.")
  }

  fs.close()
  prn(".. .. .. By")


  def prn( s: String ) : Unit = {
    println(".. .. .. .. .. .. .. .. .. .. .. .. .. .. .. .. .. .. ")
    println( s )
  }

  def appendStringToFile( file: Path, strToAppend: String) = {
    val fs_append = fs.append( file )
    val writer = new PrintWriter( fs_append )
    writer.append( strToAppend )
    writer.flush()
    fs_append.hflush()
    writer.close()
    fs_append.close()
  }
}

trait Greeting {
  lazy val greeting: String = "hello"
}
