package link.rdcn

import link.rdcn.TestBase.csvDir
import link.rdcn.client.FairdClient
import org.apache.arrow.flight.FlightRuntimeException
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class test {

  @Test
  def test1(): Unit = {
    val dc = FairdClient.connect("dacp://0.0.0.0:3101")
    val df = dc.open(csvDir + "\\data_1.csv")

//    try {
//      df.foreach(_ => {})
//    } catch {
//      case e: FlightRuntimeException => {
//        println(e.getMessage)
//        assertEquals("User not logged in", e.getMessage)
//      }

//    }
  }

}
