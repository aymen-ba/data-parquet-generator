import java.io.IOException
import java.text.SimpleDateFormat
import java.util.{Calendar, Random}

import com.github.mjakubowski84.parquet4s.{ParquetReader, ParquetWriter}

import scala.collection.mutable.ArrayBuffer


object Main extends App {

  val timeStamp = new SimpleDateFormat("yyyyMMdd_HHmmss.sssZ").format(Calendar.getInstance.getTime)

  // transaction model
  case class Transaction(timestamp: String, idProduit: String, idTransaction: String, idMagasin: String, quantite: String)


  //generate random ids magasin
  val rd: Random = new Random
  var arr: Array[Byte] = new Array[Byte](16)
  var idsMagasin = new Array[String](1200)
  var idMagasin =""

  for(i <- 0 until  1200) {
    // generate random magasin id
    rd.nextBytes(arr)
    val sb = new StringBuilder
    for (b <- arr) {
      sb.append(String.format("%02x", Byte.box(b)))
    }
    idMagasin = sb.toString
    idMagasin = idMagasin.substring(0, 8) + '-' + idMagasin.substring(8, 12) + '-' + idMagasin.substring(12, 16) + '-' + idMagasin.substring(16, 20) + '-' + idMagasin.substring(20)
    idsMagasin(i) = idMagasin
  }


  for(k <- 0 until 100){
    val path = "hdfs://127.0.0.1:/tmp/data/part_"+ k +".parquet"
    println(path)

    // generate transactions
    var transactions = ArrayBuffer[Transaction]()

    for (j <- k*1500000 until 1500000*(k+1)){
      //write transaction: timeStamp|idProduit|idTransaction|idMagasin|qte

      val repeatTransaction = rd.nextInt(9) + 1
      val indexMagasin = rd.nextInt(1200)


      for( r <- 0 until repeatTransaction){
        val productId = rd.nextInt(10000) + 1
        val qte = rd.nextInt(9) + 1

        val transaction = Transaction(timeStamp,productId.toString,(j + 1).toString,idsMagasin(indexMagasin),qte.toString)

        try {
          transactions += transaction

        } catch {
          case ex: IOException => {
            ex.printStackTrace()
          }
        }
      }
    }


    // writing
    ParquetWriter.write(path, transactions.toStream)


    // reading
    val parquetIterable = ParquetReader.read[Transaction](path)
    try {
      parquetIterable.foreach(println)
    } finally parquetIterable.close()

  }

}
