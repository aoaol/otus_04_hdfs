package example


import java.io.{ BufferedReader, InputStreamReader }

import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import java.net.URI

import org.apache.log4j.BasicConfigurator

import scala.collection.mutable.ListBuffer
import scala.io.Source


object Hello extends Greeting with App {
  val outDir = "/ods"
  val inDir = "/stage"
  val dataFileExtension = ".csv"
  val tempDestConcatName = "temp_concat_csv.tmp"
  BasicConfigurator.configure()

  prn( greeting )

  val conf = new Configuration()
  val fs = FileSystem.get( new URI("hdfs://localhost:9000"), conf)

  val outPath = new Path( outDir )
  if (! fs.exists( outPath))
      fs.mkdirs( outPath )

  prn(s" Folder $inDir")
  for( inSubDir <- fs.listStatus( new Path( inDir )) if inSubDir.isDirectory) {
    prn( s"subDir: ${inSubDir.getPath.getName}")
    val outSubDirName = s"$outDir/${inSubDir.getPath.getName}"
    val outSubDirPath = new Path( outSubDirName )
    if (! fs.exists( outSubDirPath ))
        fs.mkdirs( outSubDirPath )

    val destFiles = for( destFile <- fs.listStatus( new Path( outSubDirName ))   if destFile.isFile)    yield new Path( outSubDirName +"/"+ destFile.getPath.getName )

    val inSubDirName = s"$inDir/${inSubDir.getPath.getName}"
    val inFiles = for( inFile <- fs.listStatus( new Path( inSubDirName ))
                        if inFile.isFile   &&   inFile.getLen > 0   &&   inFile.getPath.getName.endsWith( dataFileExtension ))
      yield new Path( outSubDirName +"/"+ inFile.getPath.getName )
     //prn( inFile.toString )


    if (inFiles.nonEmpty) {
      inFiles.foreach( x => prn( s"   src file:  ${x.getName}" ))

      val destFilePath = new Path( s"$outSubDirName/" + (if (destFiles.nonEmpty)  destFiles(0).getName else inFiles(0).getName) )
      prn( s"   dest file: ${destFilePath.getName}")

      if (! fs.exists( destFilePath ))
        fs.createNewFile( destFilePath )

      val tempDestFilePath = new Path( outSubDirName +"/"+ tempDestConcatName )
      fs.rename( destFilePath, tempDestFilePath)
      inFiles.foreach( inf => fs.rename( new Path( inSubDirName +"/"+ inf.getName ), inf ))
      prn( s"   after rename inFiles")

      //val inFilesWNL
      fs.concat( tempDestFilePath, inFiles)

      fs.rename( tempDestFilePath, destFilePath )
    }
    else
      println("--->   NO data files to merge.")


  }


  //val filename = inDir + "/date=2020-12-03/part-0000.csv"
  //val path = new Path( filename )
  //val stream : FSDataInputStream = fs.open( path )
  //val source = Source.fromInputStream( stream, "UTF-8")
  //for( str <- source.getLines() )
  //  prn(s" from $filename: $str")

  // LazyList.cons(stream.read, LazyList.continually( stream.read))
  //val br = new BufferedReader( new InputStreamReader( i ));
  //val bufferedReader  = BufferedReader( )


  //stream.close()
  fs.close()

  prn(".. .. .. By")


  def prn( s: String ) : Unit = {
    println(".. .. .. .. .. .. .. .. .. .. .. .. .. .. .. .. .. .. ")
    println( s )
  }
}

trait Greeting {
  lazy val greeting: String = "hello"
}
