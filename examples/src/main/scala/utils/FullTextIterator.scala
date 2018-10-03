// package asap.examples.mllib.utils

// import scala.io._
// import java.io._

// import scala.collection.mutable.ArrayBuffer
// import scala.collection.JavaConversions._
// import java.io.File
// import java.nio.charset.CodingErrorAction
// import scala.io.Codec

// import java.io.File
// import java.io.FileInputStream
// import java.nio.ByteBuffer
// import org.apache.spark.mllib.feature.wrapByteBuffer

// object FullTextIterator extends Serializable {

//   val trace = false


//   class ASAPWordsIterableOHeap[String](val dname: java.lang.String, val fname: java.lang.String) extends Iterable[wrapByteBuffer] with Serializable {
//     def iterator = new ASAPWordsIteratorOHeap(dname, fname)
//   }

//   // object ASAPWordsIterableOHeap {
//   // def apply(dname: String, fname: String) = new ASAPWordsIterableOHeap[wrapByteBuffer](dname, fname)
//   // }

//   class ASAPFileIterableOHeap[ASAPWordsIterableOHeap](val dname: String) extends Iterable[DirectoryIteratorOHeap.ASAPWordsIterableOHeap[wrapByteBuffer]] with Serializable {
//     if (trace) println("dname is " + dname)
//     def fileIterator = new File(dname).listFiles
//     // if (trace) fileIterator.foreach(println)

//     // def iterator = fileIterator.toIterator.map { fI => ASAPWordsIterableOHeap(dname, fI.getName) }
//     def iterator = fileIterator.toIterator.map { fI => createASAPWordsIterableOHeap(dname, fI.getName) }
//     def createASAPWordsIterableOHeap(dirname: String, filename: String): DirectoryIteratorOHeap.ASAPWordsIterableOHeap[wrapByteBuffer] = new DirectoryIteratorOHeap.ASAPWordsIterableOHeap(dirname, filename)
//   }

//   class ASAPWordsIteratorOHeap(val dname: String, val fname: String) extends Iterator[wrapByteBuffer] with Serializable {

//     var dirname = dname
//     var filename = fname
//     var fullname: String = dirname + "/" + filename

//     def file = new File(fullname)

//     // println("filename is "+filename)
//     val fileSize = file.length.toInt
//     val stream = new FileInputStream(file)
//     val myByteBuffer = managedFromFile(fullname)

//     def managedFromFile[T](fileName: String) = {
//       implicit val codec = Codec("UTF-8")
//       codec.onMalformedInput(CodingErrorAction.REPLACE)
//       codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
//       val myByteBuffer: ByteBuffer = ByteBuffer.allocateDirect(fileSize)
//       val bufferLen = stream.getChannel.read(myByteBuffer)
//       myByteBuffer.position(0)
//       myByteBuffer
//     }

//     def hasNext: Boolean = {
//       if (myByteBuffer.position < myByteBuffer.limit)
//         true
//       else {
//         stream.close
//         // println("Time spent in next for file " + filename + " was " + fileTime.toString())
//         false
//       }
//     }

//     var fileTime: Long = 0

//     def time[R](block: => R): R = {
//       val t0 = System.nanoTime()
//       val result = block // call-by-name
//       val t1 = System.nanoTime()
//       fileTime = fileTime + (t1 - t0)
//       // println("Next time: " + (t1 - t0) + "ns")
//       result
//     }

//     def next(): wrapByteBuffer = {
//       time {
//         if (myByteBuffer.position >= myByteBuffer.limit)
//           throw new Exception("Calling next end of buffer")
//         val newtest = myByteBuffer.slice()
//         val startPos = myByteBuffer.position
//         // if (trace) println("Top newtest details: " + newtest.position + " - " + newtest.limit + " - " + newtest.capacity)
//         // if (trace) println("myByteBuffer.position is " + myByteBuffer.position)
//         // if (trace) println("myByteBuffer details: " + myByteBuffer.position + " - " + myByteBuffer.limit + " - " + myByteBuffer.capacity)
//         var myByte = myByteBuffer.get()
//         // if (trace) println("myByteBuffer details: " + myByteBuffer.position + " - " + myByteBuffer.limit + " - " + myByteBuffer.capacity)

//         if (false) {

//         // ORIGINAL VERSION, trying more efficient version in ELSE of this if

//         while ((myByteBuffer.position <= (myByteBuffer.limit)) && (myByte != 32) && (myByte > 16)) {
//           // if (trace) println("We have " + myByte + " at " + myByteBuffer.position)
//           if (myByteBuffer.position < myByteBuffer.limit) myByte = myByteBuffer.get
//           else myByte = 0.toByte
//         }
//         // if (trace) println("3 " + newtest.position + " - " + newtest.limit + " - " + newtest.capacity)

//         if (myByte == 32 || myByte < 16)
//           newtest.limit(myByteBuffer.position - startPos - 1)

//         var skipped = false
//         var spaceChar = (myByte == 32)
//         while (myByteBuffer.position < myByteBuffer.limit && (myByte < 16 || myByte == 32)) {
//           // if (trace) println("skipping Byte is " + myByte)
//           myByte = myByteBuffer.get()
//           skipped = true
//         }
//         if ((myByteBuffer.position != myByteBuffer.limit) && (skipped || spaceChar)) myByteBuffer.position(myByteBuffer.position - 1)

//         } else {

//        // TRY more efficient code reviewed version:

//         while ((myByte != 32) && (myByte > 16) && (myByteBuffer.position < (myByteBuffer.limit))) {
//           // if (trace) println("We have " + myByte + " at " + myByteBuffer.position)
//           // if (myByteBuffer.position < myByteBuffer.limit)
//           myByte = myByteBuffer.get()
//           // else myByte = 0.toByte
//         }
//         // if (myByteBuffer.position >= (myByteBuffer.limit) myByte = 0
    
//         // if (trace) println("3 " + newtest.position + " - " + newtest.limit + " - " + newtest.capacity)

//         if (myByte == 32 || myByte < 16 || myByteBuffer.position >= myByteBuffer.limit)
//           newtest.limit(myByteBuffer.position - startPos - 1)

//         var skipped = false
//         // var spaceChar = (myByte == 32)
//         while ((myByte < 16 || myByte == 32) && myByteBuffer.position < myByteBuffer.limit) {
//           // if (trace) println("skipping Byte is " + myByte)
//           myByte = myByteBuffer.get()
//           skipped = true
//         }
//         // If we have skipped white space, rewind 1 so we're at the start of the next non-white space text section
//         if ((myByteBuffer.position != myByteBuffer.limit) && (skipped)) myByteBuffer.position(myByteBuffer.position - 1)
//         }

//         // if (trace) println("4 " + newtest.position + " - " + newtest.limit + " - " + newtest.capacity)
//         new wrapByteBuffer(newtest)
//       }
//     }
//   }

//   // object ASAPWordsIteratorOHeap {
//   // def apply(dname: String, fname: String) = new ASAPWordsIteratorOHeap(dname, fname)
//   // }

//   def printAsString(word: ByteBuffer) {
//     var bytes: Array[Byte] = Array.fill[Byte](word.remaining())(0)
//     word.mark()
//     word.get(bytes, 0, bytes.length)
//     val str = new String(bytes, "UTF-8")
//     println(str)
//     word.reset()
//   }

//   /* Unit testing main: */
//   // def main(args: Array[String]): Unit = {

//   /*
//   val myDocs = new ASAPFileIterableOHeap[ASAPWordsIterableOHeap[String]]("src/testdir")

//   val out = new java.io.DataOutputStream(new java.io.ByteArrayOutputStream())

//   val oos = new ObjectOutputStream(new FileOutputStream("/tmp/nflx"))
//   oos.writeObject(myDocs)
//   oos.close

//   for (doc <- myDocs) {
//     println("File....")
//     for (word <- doc)
//       printAsString(word)

//   }
//   */

// }

// object DirectoryIterator extends Serializable {

//   class ASAPWordsIterable(val dname: java.lang.String, val fname: java.lang.String) extends Iterable[java.lang.String] with Serializable {
//     def iterator = new ASAPWordsIterator(dname, fname)
//   }

//   // object ASAPWordsIterable {
//   // def apply(dname: String, fname: String) = new ASAPWordsIterable[String](dname, fname)
//   // }

//   class ASAPFileIterable[ASAPWordsIterable](val dname: String) extends Iterable[DirectoryIterator.ASAPWordsIterable] with Serializable {
//     // println("dname is " + dname)
//     def fileIterator = new File(dname).listFiles.toIterator
//     // def iterator = fileIterator.map { fI => ASAPWordsIterable(dname, fI.getName) }
//     def iterator = fileIterator.map { fI => createASAPWordsIterable(dname, fI.getName) }
//     def createASAPWordsIterable(dirname: String, filename: String): DirectoryIterator.ASAPWordsIterable = new DirectoryIterator.ASAPWordsIterable(dirname, filename)

//   }

//   class ASAPWordsIterator(val dname: String, val fname: String) extends Iterator[String] with Serializable {

//     var dirname = dname
//     var filename = fname
//     var fullname: String = dirname + "/" + filename

//     val fileSrcMap = scala.collection.mutable.Map[String, Source]()

//     println("filename is "+filename)
//     var lineIterator = managedFromFile(fullname) { s => s.getLines }

//     var wordIterator: Iterator[String] = null
//     var nextWord: String = null

//     if (lineIterator.hasNext) {
//       wordIterator = lineIterator.next.split("\\s+").iterator
//       if (wordIterator.hasNext) nextWord = wordIterator.next
//     }

//     def managedFromFile[T](fileName: String)(f: scala.io.Source => T) = {
//       implicit val codec = Codec("UTF-8")
//       codec.onMalformedInput(CodingErrorAction.REPLACE)
//       codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

//       val src = Source.fromFile(fileName)
//       fileSrcMap(fileName) = src
//       manageSource(src)(f)
//     }

//     def manageSource[T](s: scala.io.Source)(f: scala.io.Source => T) = {
//       val t = f(s)
//       t
//     }

//     def next: String = {
//       val currentWord = nextWord
//       nextWord = nextNext()
//       currentWord
//     }

//     var fileTime: Long = 0

//     def time[R](block: => R): R = {
//       val t0 = System.nanoTime()
//       val result = block // call-by-name
//       val t1 = System.nanoTime()
//       fileTime = fileTime + (t1 - t0)
//       // println("Next time: " + (t1 - t0) + "ns")
//       result
//     }

//     def nextNext(): String = {
      
//         if (!wordIterator.hasNext) {
//           // Ignore blank lines
//           var gotNonEmptyLine = false
//           while ((!wordIterator.hasNext) && (lineIterator.hasNext) && !gotNonEmptyLine) {

//             val lineStr = lineIterator.next
//             if (lineStr.trim.length > 0) {
//               wordIterator = lineStr.trim.split("\\s+").iterator
//               gotNonEmptyLine = true
//             }
//           }
//         }
//         if (wordIterator.hasNext)
//           wordIterator.next
//         else
//           null
//       }

//     def hasNext = {
//       if (nextWord != null) {
//         true
//       } else {
//         fileSrcMap(fullname).close()
//         false
//       }
//     }
//   }

//   // object ASAPWordsIterator {
//   // def apply(dname: String, fname: String) = new ASAPWordsIterator(dname, fname)
//   // }

//   /* Unit testing main:

//     def main(args: Array[String]): Unit = {

//         val myDocs = new ASAPFileIterable("testdir")

//         for (doc <- myDocs){


//                 println("File....")
//                 for (word <- doc)
//                     println(word)

//         }
//     }
// */

// }


