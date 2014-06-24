package org.hittepit.smapapi.mapper

import org.scalatest.WordSpec
import org.scalatest.MustMatchers
import org.scalatest.mock.MockitoSugar
import java.sql.PreparedStatement
import org.mockito.Mockito._
import org.mockito.Matchers._
import org.hittepit.smapapi.mapper.Condition._
import org.hittepit.smapapi.core.Param
import org.hittepit.smapapi.core.StringProperty

class TestCondition extends WordSpec with MustMatchers with MockitoSugar {
  val prop1 = new EqualsCondition(
    new ColumnDefinition("prop1",StringProperty,{ x: Any => throw new Exception("Not used") }), "test1")
  val prop2 = new EqualsCondition(
    new ColumnDefinition("prop2",StringProperty,{ x: Any => throw new Exception("Not used") }), "test2")
  val prop3 = new EqualsCondition(
    new ColumnDefinition("prop3",StringProperty,{ x: Any => throw new Exception("Not used") }), "test3")
  val prop4 = new EqualsCondition(
    new ColumnDefinition("prop4",StringProperty,{ x: Any => throw new Exception("Not used") }), "test4")
  val and1 = and(prop1, prop2, prop3)
  trait TestEnvironment {
    val ps = mock[PreparedStatement]
  }

//  "A property condition" when {
//    "creating the sql fragment" must {
//      "return prop = ?" in {
//        val s = prop1.sqlString()
//        s must be("prop1=?")
//      }
//    }
//
//    "setting parameters" must {
//      "set the value at right index" in new TestEnvironment {
//        prop1.setParameter(2, ps)
//        verify(ps, times(1)).setString(3, "test1")
//        verify(ps, times(1)).setString(anyInt, anyString)
//      }
//      "return the current index" in new TestEnvironment {
//        val i = prop1.setParameter(2, ps)
//        i must be(3)
//      }
//    }
//  }
//
//  "An 'and' condition defined with PropertyConditions " when {
//    "creating the sql fragment" must {
//      "return a sql fragment where property fragments are sepered with and" in {
//        val s = and1.sqlString()
//        s must be("prop1=? and prop2=? and prop3=?")
//      }
//    }
//
//    "setting parameters" must {
//      "set the values at right index" in new TestEnvironment {
//        and1.setParameter(2, ps)
//        verify(ps, times(1)).setString(3, "test1")
//        verify(ps, times(1)).setString(4, "test2")
//        verify(ps, times(1)).setString(5, "test3")
//        verify(ps, times(3)).setString(anyInt, anyString)
//      }
//      "return the current index" in new TestEnvironment {
//        val i = and1.setParameter(2, ps)
//        i must be(5)
//      }
//    }
//  }
//
//  "An 'and' condition defined with only one propertyCondition" when {
//    val fakeAnd = and(prop1)
//    "creating the sql fragment" must {
//      "only produce the propertyCondition sql fragment" in {
//        val s = fakeAnd.sqlString()
//        s must be("prop1=?")
//      }
//    }
//
//    "setting paremeters" must {
//      "set only one value, the one of the property index" in new TestEnvironment {
//        fakeAnd.setParameter(3, ps)
//        verify(ps, times(1)).setString(4, "test1")
//        verify(ps, times(1)).setString(anyInt, anyString)
//      }
//      "return the current index" in new TestEnvironment {
//        val i = fakeAnd.setParameter(3, ps)
//        i must be(4)
//      }
//    }
//  }
//
//  "A combination of 'and' and 'or'" when {
//    "sqlString of and(p1, or(p2,p3))" must {
//      "return p1 and (p2 or p3)" in new TestEnvironment {
//        val or1 = or(prop2, prop3)
//        val and1 = and(prop1, or1)
//        and1.sqlString must be("prop1=? and (prop2=? or prop3=?)")
//      }
//    }
//    "sqlString of or(and(p1, p2), p3)" must {
//      "return p1 and p2 or p3" in new TestEnvironment {
//        val and1 = and(prop1, prop2)
//        val or1 = or(and1, prop3)
//        or1.sqlString must be("prop1=? and prop2=? or prop3=?")
//      }
//    }
//    "sqlString of and(or(p1, p2), p3)" must {
//      "return (p1 or p2) and or p3" in new TestEnvironment {
//        val or1 = or(prop1, prop2)
//        val and1 = and(or1, prop3)
//        and1.sqlString must be("(prop1=? or prop2=?) and prop3=?")
//      }
//    }
//    "sqlString of or(p1, and(p2, p3))" must {
//      "return p1 or p2 and p3" in new TestEnvironment {
//        val and1 = and(prop2, prop3)
//        val or1 = or(prop1, and1)
//        or1.sqlString must be("prop1=? or prop2=? and prop3=?")
//      }
//    }
//  }
//
//  "The not condition" when {
//    "not(p1=x)" must {
//      val c = Condition.not(prop1)
//      "return not p1=?" in {
//        c.sqlString() must be("not prop1=?")
//      }
//      "set only one parameter and index+1" in new TestEnvironment {
//        val index = c.setParameter(3, ps)
//        verify(ps, times(1)).setString(4, "test1")
//        verify(ps, times(1)).setString(anyInt, anyString)
//        index must be(4)
//      }
//    }
//    "not(p1 and p2)" must {
//      val c = Condition.not(Condition.and(prop1, prop2))
//      "return not (p1=? and p2=?" in {
//        c.sqlString() must be("not (prop1=? and prop2=?)")
//      }
//      "set the two parameters and return index+2" in new TestEnvironment {
//        val index = c.setParameter(3, ps)
//        verify(ps, times(1)).setString(4, "test1")
//        verify(ps, times(1)).setString(5, "test2")
//        verify(ps, times(2)).setString(anyInt, anyString)
//        index must be(5)
//      }
//    }
//  }
//
//  "The in condition" when {
//    val cd = new ColumnDefinition("prop1",NotNullableVarchar,{ x: Any => throw new Exception("Not used") })
//    "given a list of values" must {
//      val c = Condition.in(cd, "test1", "test2", "test3", "test4")
//      "generated prop in (v1,v2,v3...) sql string" in {
//        c.sqlString() must be("prop1 in (?,?,?,?)")
//      }
//      "set the parameters and return a correct new index" in new TestEnvironment {
//        c.setParameter(3, ps) must be(7)
//        verify(ps, times(4)).setString(anyInt, anyString)
//        verify(ps, times(1)).setString(4, "test1")
//        verify(ps, times(1)).setString(5, "test2")
//        verify(ps, times(1)).setString(6, "test3")
//        verify(ps, times(1)).setString(7, "test4")
//      }
//    }
//    "given a single value" must {
//      val c = Condition.in(cd, "test1")
//      "generated prop in (v1) sql string" in {
//        c.sqlString() must be("prop1 in (?)")
//      }
//      "set the parameters and return a correct new index" in new TestEnvironment {
//        c.setParameter(3, ps) must be(4)
//        verify(ps, times(1)).setString(anyInt, anyString)
//        verify(ps, times(1)).setString(4, "test1")
//      }
//    }
//  }
//  
//  "A sql condition" when {
//    "used alone" must {
//      val c = sql("id = ? and toto > ? or a <> ?", List(Param(25,IntPropertyeger),Param("xxx",NotNullableVarchar),Param(Some(12.3),OptionalDoubleProperty)))
//      "simply generate given sql" in new TestEnvironment {
//        c.sqlString() must be("id = ? and toto > ? or a <> ?")
//      }
//      
//      "set the parameters correctly" in new TestEnvironment {
//        val i = c.setParameter(3, ps)
//        verify(ps,times(1)).setInt(4, 25)
//        verify(ps,times(1)).setString(5, "xxx")
//        verify(ps,times(1)).setDouble(6, 12.3)
//        i must be(6)
//      }
//    }
//    
//    "combined with other conditions" must{
//      val c = and(prop1,sql("id = ? and toto > ? or a <> ?", List(Param(25,IntPropertyeger),Param("xxx",NotNullableVarchar),Param(Some(12.3),OptionalDoubleProperty))),prop2)
//      "generate de correct sql" in new TestEnvironment {
//        c.sqlString() must be("prop1=? and (id = ? and toto > ? or a <> ?) and prop2=?")
//      }
//      
//      "set the paremeters correctly" in new TestEnvironment {
//        val i = c.setParameter(3, ps)
//        verify(ps,times(1)).setString(4, "test1")
//        verify(ps,times(1)).setInt(5, 25)
//        verify(ps,times(1)).setString(6, "xxx")
//        verify(ps,times(1)).setDouble(7, 12.3)
//        verify(ps,times(1)).setString(8, "test2")
//        i must be(8)
//      }
//    }
//  }
}