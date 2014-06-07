package org.hittepit.smapapi.mapper

import org.scalatest.WordSpec
import org.scalatest.MustMatchers
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._
import java.sql.ResultSet
import org.scalatest.GivenWhenThen
import java.sql.PreparedStatement
import java.sql.Types
import org.hittepit.smapapi.core.NullableVarchar
import org.hittepit.smapapi.core.NotNullableVarchar

class TestVarcharSqlType extends WordSpec with MustMatchers with MockitoSugar {
	"The columnValue method of a NotNullableVarchar" when {
	  "called for a varchar column containing a not null value" must {
	    "return a String with the value" in {
		  implicit val rs = mock[ResultSet]
		  when(rs.getString("test")).thenReturn("hello")
		  val v = NotNullableVarchar.getColumnValue(rs,Left("test"))
		  v must be("hello")
	    }
	  }
	  "called for a varchar column containing null" must {
	    "throw a NullValueException" in {
		  implicit val rs = mock[ResultSet]
		  when(rs.getString("test")).thenReturn(null)
		  when(rs.wasNull()).thenReturn(true)
		  
		  an [NullValueException] must be thrownBy NotNullableVarchar.getColumnValue(rs,Left("test"))
	    }
	  }
	}
  
	"The columnValue method of a NullableVarchar" when {
	  "called for a varchar column containing a not null value" must {
	    "return Some(String) with the value" in {
		  implicit val rs = mock[ResultSet]
		  when(rs.getString("test")).thenReturn("hello")
		  val v = NullableVarchar.getColumnValue(rs,Left("test"))
		  v must be(Some("hello"))
	    }
	  }
	  "called for a varchar column containing null" must {
	    "return None" in {
		  implicit val rs = mock[ResultSet]
		  when(rs.getString("test")).thenReturn(null)
		  when(rs.wasNull()).thenReturn(true)
		  
		  val v = NullableVarchar.getColumnValue(rs,Left("test"))
		  v must be(None)
	    }
	  }
	}
	
	"The setParameter method of a NullableVarchar" when{
	  "receiving a Some(String) and a PreparedStatement" must {
	    "set the string on the preparedStatement" in {
	      implicit val ps = mock[PreparedStatement]
	      NullableVarchar.setColumnValue(1, Some("hello"),ps)
	      verify(ps).setString(1,"hello")
	    }
	  }
	  "receiving None and a PreparedStatement" must {
	    "set null on the PreparedStatement" in {
	      implicit val ps = mock[PreparedStatement]
	      NullableVarchar.setColumnValue(1, None,ps)
	      verify(ps).setNull(1,Types.VARCHAR)
	    }
	  }
	}
	
	"The setParameter method of a NotNullableVarchar" when{
	  "receiving a Some(String) and a PreparedStatement" must {
	    "set the string on the preparedStatement" in {
	      implicit val ps = mock[PreparedStatement]
	      NotNullableVarchar.setColumnValue(1, "hello",ps)
	      verify(ps).setString(1,"hello")
	    }
	  }
	}
}