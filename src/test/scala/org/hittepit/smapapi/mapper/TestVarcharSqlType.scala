package org.hittepit.smapapi.mapper

import org.scalatest.WordSpec
import org.scalatest.MustMatchers
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._
import java.sql.ResultSet
import org.scalatest.GivenWhenThen
import java.sql.PreparedStatement
import java.sql.Types

class TestVarcharSqlType extends WordSpec with MustMatchers with MockitoSugar {
	"The columnValue method of a NotNullableVarchar" when {
	  "called for a varchar column containing a not null value" must {
	    "return a String with the value" in {
		  implicit val rs = mock[ResultSet]
		  when(rs.getString("test")).thenReturn("hello")
		  val v = NotNullableVarchar.columnValue("test")
		  v must be("hello")
	    }
	  }
	  "called for a varchar column containing null" must {
	    "throw a NullValueException" in {
		  implicit val rs = mock[ResultSet]
		  when(rs.getString("test")).thenReturn(null)
		  when(rs.wasNull()).thenReturn(true)
		  
		  an [NullValueException] must be thrownBy NotNullableVarchar.columnValue("test")
	    }
	  }
	}
  
	"The columnValue method of a NullableVarchar" when {
	  "called for a varchar column containing a not null value" must {
	    "return Some(String) with the value" in {
		  implicit val rs = mock[ResultSet]
		  when(rs.getString("test")).thenReturn("hello")
		  val v = NullableVarchar.columnValue("test")
		  v must be(Some("hello"))
	    }
	  }
	  "called for a varchar column containing null" must {
	    "return None" in {
		  implicit val rs = mock[ResultSet]
		  when(rs.getString("test")).thenReturn(null)
		  when(rs.wasNull()).thenReturn(true)
		  
		  val v = NullableVarchar.columnValue("test")
		  v must be(None)
	    }
	  }
	}
	
	"The setParameter method of a NullableVarchar" when{
	  "receiving a Some(String) and a PreparedStatement" must {
	    "set the string on the preparedStatement" in {
	      implicit val ps = mock[PreparedStatement]
	      NullableVarchar.setParameter(1, Some("hello"))
	      verify(ps).setString(1,"hello")
	    }
	  }
	  "receiving None and a PreparedStatement" must {
	    "set null on the PreparedStatement" in {
	      implicit val ps = mock[PreparedStatement]
	      NullableVarchar.setParameter(1, None)
	      verify(ps).setNull(1,Types.VARCHAR)
	    }
	  }
	}
	
	"The setParameter method of a NotNullableVarchar" when{
	  "receiving a Some(String) and a PreparedStatement" must {
	    "set the string on the preparedStatement" in {
	      implicit val ps = mock[PreparedStatement]
	      NotNullableVarchar.setParameter(1, "hello")
	      verify(ps).setString(1,"hello")
	    }
	  }
	}
}