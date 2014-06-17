package org.hittepit.smapapi.core

import org.scalatest.WordSpec
import org.scalatest.MustMatchers

class TestParam extends WordSpec with MustMatchers {
	"The Param factory" when {
	  "called with a String value and a PropertyType" must {
	    "return a Param object" in {
	      val p = Param("test",NotNullableString)
	      p.value must be("test")
	      p.propertyType must be(NotNullableString)
	    }
	  }
	  "called with a Int value and a PropertyType" must {
	    "return a Param object" in {
	      val p = Param(2,NotNullableInt)
	      p.value must be(2)
	      p.propertyType must be(NotNullableInt)
	    }
	  }
	}
}