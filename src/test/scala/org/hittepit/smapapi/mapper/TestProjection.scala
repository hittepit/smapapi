package org.hittepit.smapapi.mapper

import org.scalatest.WordSpec
import org.scalatest.MustMatchers
import org.hittepit.smapapi.core.NotNullableString

class TestProjection extends WordSpec with MustMatchers {
  val c1 = new ColumnDefinition("c1", NotNullableString, { x: Any => throw new Exception("Not used") })
  val c2 = new ColumnDefinition("c2", NotNullableString, { x: Any => throw new Exception("Not used") })
  val c3 = new ColumnDefinition("c3", NotNullableString, { x: Any => throw new Exception("Not used") })
  val c4 = new ColumnDefinition("c4", NotNullableString, { x: Any => throw new Exception("Not used") })

  "A projection" when {
    "created with a simple column" must {
      val p = Projection((None, c1))
      "generate the name of the column" in {
        p.sqlString must be("c1")
      }
    }
    "created with a simple aliased column" must {
      val p = Projection((Some("col1"), c1))
      "generate the name of the column as alias" in {
        p.sqlString must be("c1 as col1")
      }
    }
    "created with a list of simple columns" must {
      val p = Projection((None, c1), (None, c2), (None, c3), (None, c4))
      "generate a list of columns separeted by a comma" in {
        p.sqlString must be("c1,c2,c3,c4")
      }
    }
    "created with a list of aliased columns" must {
      val p = Projection((Some("col1"), c1), (Some("col2"), c2), (Some("col3"), c3), (Some("col4"), c4))
      "generate a list of columns with theirs aliases separeted by a comma" in {
        p.sqlString must be("c1 as col1,c2 as col2,c3 as col3,c4 as col4")
      }
    }
    "created with a mixed list of simple columns and alias columns" must {
      val p = Projection((Some("col1"), c1), (None, c2), (Some("col3"), c3), (None, c4))
      "generate the correct list of columns, aliased or not, separeted by a comma" in {
        p.sqlString must be("c1 as col1,c2,c3 as col3,c4")
      }
    }
  }

  "A count projection" when {
    "created without any parameters" must {
      "generate count(*)" in {
        val c = new CountProjection((None,None))
        c.sqlString must be("count(*)")
      }
    }
    "created with a column name and no alias" must {
      "generate count(column)" in {
        val c = new CountProjection((None,Some("test")))
        c.sqlString must be("count(test)")
      }
    }
    "created with a column name and an alias" must {
      "generate count(column) as alias" in {
        val c = new CountProjection((Some("t"),Some("test")))
        c.sqlString must be("count(test) as t")
      }
    }
    "created with no column name and an alias" must {
      "generate count(*) as alias" in {
        val c = new CountProjection((Some("t"),None))
        c.sqlString must be("count(*) as t")
      }
    }
    "created with a inner projection" must {
      "generate count(innerProjection)" in {
        pending
      }
    }
  }
}