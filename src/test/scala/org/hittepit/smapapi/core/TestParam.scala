package org.hittepit.smapapi.core

import org.scalatest.WordSpec
import org.scalatest.MustMatchers

class TestParam extends WordSpec with MustMatchers {
	"The Param factory" when {
	  "called with a value and a PropertyType" must {
	    "return a Param object" in {
	      val p = Param("test",NotNullableString)
	      p.value must be("test")
	      p.propertyType must be(NotNullableString)
	    }
	  }
	  "called with a value" must {
	    "return a Param object" in {
	      val p = Param("test")
	      p.value must be("test")
	      p.propertyType must be(NotNullableString)
	    }
	  }
	  "called with a optional value" must {
	    "throw an exception" in {
	      an [Exception] must be thrownBy(Param(Some("test")))
	    }
	  }
	}
}