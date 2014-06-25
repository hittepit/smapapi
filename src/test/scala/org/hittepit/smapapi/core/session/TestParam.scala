package org.hittepit.smapapi.core.session

import org.scalatest.WordSpec
import org.scalatest.MustMatchers
import org.hittepit.smapapi.core.StringProperty
import org.hittepit.smapapi.core.IntProperty

class TestParam extends WordSpec with MustMatchers {
	"The Param factory" when {
	  "called with a String value and a PropertyType" must {
	    "return a Param object" in {
	      val p = Param("test",StringProperty)
	      p.value must be("test")
	      p.propertyType must be(StringProperty)
	    }
	  }
	  "called with a Int value and a PropertyType" must {
	    "return a Param object" in {
	      val p = Param(2,IntProperty)
	      p.value must be(2)
	      p.propertyType must be(IntProperty)
	    }
	  }
	}
}