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
	"The columnValue method of a simple Varchar" when {
	  "called for a varchar column containing a not null value" must {
	    "return a String with the value" in {
		  implicit val rs = mock[ResultSet]
		  when(rs.getString("test")).thenReturn("hello")
		  val v = Varchar.columnValue("test")
		  v must be("hello")
	    }
	  }
	  "called for a varchar column containing null" must {
	    "throw a NullValueException" in {
		  implicit val rs = mock[ResultSet]
		  when(rs.getString("test")).thenReturn(null)
		  when(rs.wasNull()).thenReturn(true)
		  
		  an [NullValueException] must be thrownBy Varchar.columnValue("test")
	    }
	  }
	}
  
	"The columnValue method of a NotNullable Varchar" when {
	  "called for a varchar column containing a not null value" must {
	    "return a String with the value" in {
		  implicit val rs = mock[ResultSet]
		  when(rs.getString("test")).thenReturn("hello")
		  val v = NotNullable(Varchar).columnValue("test")
		  v must be("hello")
	    }
	  }
	  "called for a varchar column containing null" must {
	    "throw a NullValueException" in {
		  implicit val rs = mock[ResultSet]
		  when(rs.getString("test")).thenReturn(null)
		  when(rs.wasNull()).thenReturn(true)
		  
		  an [NullValueException] must be thrownBy NotNullable(Varchar).columnValue("test")
	    }
	  }
	}
  
	"The columnValue method of a Nullable Varchar" when {
	  "called for a varchar column containing a not null value" must {
	    "return Some(String) with the value" in {
		  implicit val rs = mock[ResultSet]
		  when(rs.getString("test")).thenReturn("hello")
		  val v = Nullable(Varchar).columnValue("test")
		  v must be(Some("hello"))
	    }
	  }
	  "called for a varchar column containing null" must {
	    "return None" in {
		  implicit val rs = mock[ResultSet]
		  when(rs.getString("test")).thenReturn(null)
		  when(rs.wasNull()).thenReturn(true)
		  
		  val v = Nullable(Varchar).columnValue("test")
		  v must be(None)
	    }
	  }
	}
	
	"The setParameter method of a simple Varchar" when{
	  val sqlTypeUnderTest = Varchar
	  "receiving a Some(String) and a PreparedStatement" must {
	    "set the string on the preparedStatement" in {
	      val ps = mock[PreparedStatement]
	      sqlTypeUnderTest.setParameter(1, ps, Some("hello"))
	      verify(ps).setString(1,"hello")
	    }
	  }
	  "receiving None and a PreparedStatement" must {
	    "set null on the PreparedStatement" in {
	      val ps = mock[PreparedStatement]
	      sqlTypeUnderTest.setParameter(1, ps, None)
	      verify(ps).setNull(1,Types.VARCHAR)
	    }
	  }
	  "receiving Some(anything but a String) and a PreparedStatement" must {
	    "throw an exception" in {
	      val ps = mock[PreparedStatement]
	      an [Exception] must be thrownBy sqlTypeUnderTest.setParameter(1,ps,Some(1))
	    }
	  }
	}
	
	"The setParameter method of a Nullable Varchar" when{
	  val sqlTypeUnderTest = Nullable(Varchar)
	  "receiving a Some(String) and a PreparedStatement" must {
	    "set the string on the preparedStatement" in {
	      val ps = mock[PreparedStatement]
	      sqlTypeUnderTest.setParameter(1, ps, Some("hello"))
	      verify(ps).setString(1,"hello")
	    }
	  }
	  "receiving None and a PreparedStatement" must {
	    "set null on the PreparedStatement" in {
	      val ps = mock[PreparedStatement]
	      sqlTypeUnderTest.setParameter(1, ps, None)
	      verify(ps).setNull(1,Types.VARCHAR)
	    }
	  }
	  "receiving Some(anything but a String) and a PreparedStatement" must {
	    "throw an exception" in {
	      val ps = mock[PreparedStatement]
	      an [Exception] must be thrownBy sqlTypeUnderTest.setParameter(1,ps,Some(1))
	    }
	  }
	}
}