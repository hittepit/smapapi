package org.hittepit.smapapi.core

import org.scalatest.WordSpec
import org.scalatest.MustMatchers
import org.scalatest.mock.MockitoSugar
import java.sql.Connection

class TestSessionBehavious extends WordSpec with MustMatchers with MockitoSugar {
  "A session object" when {
    "created" must {
      "not be closed" in {
    	val session = new Session(mock[Connection]) with UpdatableSession
        session.closed must be(false)
      }
    }
    "be closed" must {
      "be closed :-)" in {
    	val session = new Session(mock[Connection]) with UpdatableSession
        session.close
        session.closed must be(true)
      }
    }
  }
  "A closed session object" when {
    "close is called" must {
      "throw an AssertionError" in {
    	val session = new Session(mock[Connection]) with UpdatableSession
	    session.close
        an[AssertionError] must be thrownBy (session.close)
      }
    }
    "commit is called" must {
      "throw an AssertionError" in {
    	val session = new Session(mock[Connection]) with UpdatableSession
	    session.close
        an[AssertionError] must be thrownBy (session.commit)
      }
    }
    "execute is called" must {
      "throw an AssertionError" in {
    	val session = new Session(mock[Connection]) with UpdatableSession
	    session.close
        an[AssertionError] must be thrownBy (session.execute(""))
      }
    }
    "insert (without generated id) is called" must {
      "throw an AssertionError" in {
    	val session = new Session(mock[Connection]) with UpdatableSession
	    session.close
        an[AssertionError] must be thrownBy (session.insert(""))
      }
    }
    "insert (with generated id) is called" must {
      "throw an AssertionError" in {
    	val session = new Session(mock[Connection]) with UpdatableSession
	    session.close
        an[AssertionError] must be thrownBy (session.insert("",List(),Column("",NotNullableInt)))
      }
    }
    "list is called" must {
      "throw an AssertionError" in {
    	val session = new Session(mock[Connection]) with UpdatableSession
	    session.close
        an[AssertionError] must be thrownBy (session.list("", mapper=r => Unit))
      }
    }
    "rollback is called" must {
      "throw an AssertionError" in {
    	val session = new Session(mock[Connection]) with UpdatableSession
	    session.close
        an[AssertionError] must be thrownBy (session.rollback)
      }
    }
    "select is called" must {
      "throw an AssertionError" in {
    	val session = new Session(mock[Connection]) with UpdatableSession
	    session.close
        an[AssertionError] must be thrownBy (session.select(""))
      }
    }
    "unique is called" must {
      "throw an AssertionError" in {
    	val session = new Session(mock[Connection]) with UpdatableSession
	    session.close
        an[AssertionError] must be thrownBy (session.unique("",mapper= r => Unit))
      }
    }
  }
}