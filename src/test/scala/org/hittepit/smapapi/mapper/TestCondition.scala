package org.hittepit.smapapi.mapper

import org.scalatest.WordSpec
import org.scalatest.MustMatchers
import org.scalatest.mock.MockitoSugar
import java.sql.PreparedStatement

import org.mockito.Mockito._

class TestCondition extends WordSpec with MustMatchers with MockitoSugar {
  trait TestEnvironment {
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
    val ps = mock[PreparedStatement]
  }

  "A property condition" when {
    "creating the sql fragment" must {
      "return prop = ?" in new TestEnvironment {
        val s = prop1.sqlString()
        s must be("prop1=?")
      }
    }

    "setting parameters" must {
      "set the value at right index" in new TestEnvironment {
        prop1.setParameter(2, ps)
        verify(ps, times(1)).setString(3, "test1")
      }
      "return the current index" in new TestEnvironment {
        val i = prop1.setParameter(2, ps)
        i must be(3)
      }
    }
  }

  "An 'and' condition defined with PropertyConditions " when {
    "creating the sql fragment" must {
      "return a sql fragment where property fragments are sepered with and" in new TestEnvironment {
    	  val s = and.sqlString()
    	  s must be("prop1=? and prop2=? and prop3=?")
      }
    }
  }
}