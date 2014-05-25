package org.hittepit.smapapi.mapper

import org.scalatest.WordSpec
import org.scalatest.MustMatchers
import org.scalatest.mock.MockitoSugar
import java.sql.ResultSet
import org.mockito.Mockito._
import java.sql.PreparedStatement
import java.sql.Types

class TestIntegerSqlType extends WordSpec with MustMatchers with MockitoSugar{
	"The columnValue method of a  simple Integer" when {
	  "called for an integer column containing a not null value" must {
	    "return an Int of the value" in {
			  implicit val rs = mock[ResultSet]
			  when(rs.getInt("test")).thenReturn(1)
			  
			  val v = Integer.columnValue("test")
			  v must be(1)
	    }
	  }
	  "called for a integer column containing null" must {
	    "throw a NullValueException" in {
		  implicit val rs = mock[ResultSet]
		  when(rs.getInt("test")).thenReturn(0)
		  when(rs.wasNull()).thenReturn(true)
		  
		  an [NullValueException] must be thrownBy Varchar.columnValue("test")
	    }
	  }
	}
	
	"The columnValue method of a Nullable Integer" when {
	  "called for an integer column containing a not null value" must {
	    "return Some(Int) of the value" in {
			  implicit val rs = mock[ResultSet]
			  when(rs.getInt("test")).thenReturn(1)
			  
			  val v = Nullable(Integer).columnValue("test")
			  v must be(Some(1))
	    }
	  }
	  "called for a integer column containing null" must {
	    "return None" in {
		  implicit val rs = mock[ResultSet]
		  when(rs.getInt("test")).thenReturn(0)
		  when(rs.wasNull()).thenReturn(true)
			  
		  val v = Nullable(Integer).columnValue("test")
		  v must be(None)
	    }
	  }
	}
	
	"The setParameter method of a simple Varchar" when{
	  "receiving a Some(Int) and a PreparedStatement" must {
	    "set the int on the preparedStatement" in {
	      val ps = mock[PreparedStatement]
	      Integer.setParameter(1, ps, Some(17))
	      verify(ps).setInt(1,17)
	    }
	  }
	  "receiving None and a PreparedStatement" must {
	    "set null on the PreparedStatement" in {
	      val ps = mock[PreparedStatement]
	      Integer.setParameter(1, ps, None)
	      verify(ps).setNull(1,Types.INTEGER)
	    }
	  }
	  "receiving Some(anything but a Int) and a PreparedStatement" must {
	    "throw an exception" in {
	      val ps = mock[PreparedStatement]
	      an [Exception] must be thrownBy Integer.setParameter(1,ps,Some("hello"))
	    }
	  }
	}
}