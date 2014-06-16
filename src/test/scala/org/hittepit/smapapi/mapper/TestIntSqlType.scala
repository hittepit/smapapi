package org.hittepit.smapapi.mapper

import org.scalatest.WordSpec
import org.scalatest.MustMatchers
import org.scalatest.mock.MockitoSugar
import java.sql.ResultSet
import org.mockito.Mockito._
import java.sql.PreparedStatement
import java.sql.Types
import org.hittepit.smapapi.core.NullableInt
import org.hittepit.smapapi.core.NotNullableInt

class TestIntPropertyType extends WordSpec with MustMatchers with MockitoSugar{
	"The columnValue method of a  NotNullableInteger" when {
	  "called for an integer column containing a not null value" must {
	    "return an Int of the value" in {
			  implicit val rs = mock[ResultSet]
			  when(rs.getInt("test")).thenReturn(1)
			  
			  val v = NotNullableInt.getColumnValue(rs,Left("test"))
			  v must be(1)
	    }
	  }
	  "called for a integer column containing null" must {
	    "throw a NullValueException" in {
		  implicit val rs = mock[ResultSet]
		  when(rs.getInt("test")).thenReturn(0)
		  when(rs.wasNull()).thenReturn(true)
		  
		  an [NullValueException] must be thrownBy NotNullableInt.getColumnValue(rs,Left("test"))
	    }
	  }
	}
	
	"The columnValue method of a NullableInteger" when {
	  "called for an integer column containing a not null value" must {
	    "return Some(Int) of the value" in {
			  implicit val rs = mock[ResultSet]
			  when(rs.getInt("test")).thenReturn(1)
			  
			  val v = NullableInt.getColumnValue(rs,Left("test"))
			  v must be(Some(1))
	    }
	  }
	  "called for a integer column containing null" must {
	    "return None" in {
		  implicit val rs = mock[ResultSet]
		  when(rs.getInt("test")).thenReturn(0)
		  when(rs.wasNull()).thenReturn(true)
			  
		  val v = NullableInt.getColumnValue(rs,Left("test"))
		  v must be(None)
	    }
	  }
	}
	
	"The setParameter method of a simple NullableInteger" when{
	  "receiving a Some(Int) and a PreparedStatement" must {
	    "set the int on the preparedStatement" in {
	      implicit val ps = mock[PreparedStatement]
	      NullableInt.setColumnValue(1, Some(17),ps)
	      verify(ps).setInt(1,17)
	    }
	  }
	  "receiving None and a PreparedStatement" must {
	    "set null on the PreparedStatement" in {
	      implicit val ps = mock[PreparedStatement]
	      NullableInt.setColumnValue(1, None,ps)
	      verify(ps).setNull(1,Types.INTEGER)
	    }
	  }
	}
}