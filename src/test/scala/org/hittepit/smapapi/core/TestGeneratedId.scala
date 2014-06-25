package org.hittepit.smapapi.core

import org.scalatest.MustMatchers
import org.scalatest.WordSpec

class TestGeneratedId extends WordSpec with MustMatchers{
	"GeneratedId" when {
	  "used" must {
	    "accept 'for' comprehension" in {
	      def modulo2IntId(i:Int) = if(i%2 == 0) Persistent(i) else Transient[Int]()
	      def modulo3StringId(i:Int) = if(i%3 == 0) Persistent(i.toString) else Transient[String]()
	      
	      val i1 = for(i <- modulo2IntId(1); j <- modulo3StringId(i)) yield(j)
	      i1 must be(Transient())
	      val i2 = for(i <- modulo2IntId(2); j <- modulo3StringId(i)) yield(j)
	      i2 must be(Transient())
	      val i3 = for(i <- modulo2IntId(3); j <- modulo3StringId(i)) yield(j)
	      i3 must be(Transient())
	      val i4 = for(i <- modulo2IntId(6); j <- modulo3StringId(i)) yield(j)
	      i4 must be(Persistent("6"))
	    }
	  }
	}
}