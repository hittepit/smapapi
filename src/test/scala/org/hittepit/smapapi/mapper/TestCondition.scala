package org.hittepit.smapapi.mapper

import org.scalatest.WordSpec
import org.scalatest.MustMatchers
import org.scalatest.mock.MockitoSugar
import java.sql.PreparedStatement

import org.mockito.Mockito._
import org.mockito.Matchers._

class TestCondition extends WordSpec with MustMatchers with MockitoSugar {
    val prop1 = new PropertyCondition(
      new ColumnDefinition[Any, String] {
        val name = "prop1";
        val sqlType = NotNullableVarchar;
        val getter = { x: Any => throw new Exception("Not used") }
      }, "test1")
    val prop2 = new PropertyCondition(
      new ColumnDefinition[Any, String] {
        val name = "prop2";
        val sqlType = NotNullableVarchar;
        val getter = { x: Any => throw new Exception("Not used") }
      }, "test2")
    val prop3 = new PropertyCondition(
      new ColumnDefinition[Any, String] {
        val name = "prop3";
        val sqlType = NotNullableVarchar;
        val getter = { x: Any => throw new Exception("Not used") }
      }, "test3")
    val and = new AndCondition(prop1,prop2,prop3)
  trait TestEnvironment {
    val ps = mock[PreparedStatement]
  }

  "A property condition" when {
    "creating the sql fragment" must {
      "return prop = ?" in {
        val s = prop1.sqlString()
        s must be("prop1=?")
      }
    }

    "setting parameters" must {
      "set the value at right index" in new TestEnvironment {
        prop1.setParameter(2, ps)
        verify(ps, times(1)).setString(3, "test1")
        verify(ps,times(1)).setString(anyInt,anyString)
      }
      "return the current index" in new TestEnvironment {
        val i = prop1.setParameter(2, ps)
        i must be(3)
      }
    }
  }

  "An 'and' condition defined with PropertyConditions " when {
    "creating the sql fragment" must {
      "return a sql fragment where property fragments are sepered with and" in {
    	  val s = and.sqlString()
    	  s must be("prop1=? and prop2=? and prop3=?")
      }
    }
    
    "setting parameters" must {
      "set the values at right index" in new TestEnvironment {
        and.setParameter(2, ps)
        verify(ps, times(1)).setString(3, "test1")
        verify(ps, times(1)).setString(4, "test2")
        verify(ps, times(1)).setString(5, "test3")
        verify(ps,times(3)).setString(anyInt,anyString)
      }
      "return the current index" in new TestEnvironment {
        val i = and.setParameter(2, ps)
        i must be(5)
      }
    }
  }
  
  "An 'and' condition defined with only one propertyCondition" when {
    val fakeAnd = new AndCondition(prop1)
    "creating the sql fragment" must {
      "only produce the propertyCondition sql fragment" in {
        val s = fakeAnd.sqlString()
        s must be("prop1=?")
      }
    }
    
    "setting paremeters" must {
      "set only one value, the one of the property index" in new TestEnvironment{
        fakeAnd.setParameter(3, ps)
        verify(ps,times(1)).setString(4, "test1")
        verify(ps,times(1)).setString(anyInt,anyString)
      }
      "return the current index" in new TestEnvironment{
        val i = fakeAnd.setParameter(3, ps)
        i must be (4)
      }
    }
  }
}