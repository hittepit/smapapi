package org.hittepit.smapapi.core

/**
 * Class used to defined an generated id property in an entity object
 */
sealed abstract class GeneratedId[T] {
	/**
	 * Returns if the id was generated or not, in other words if the entity containing it is transient or persistent.
	 * @return true if generated, false otherwise
	 */
	def isGenerated:Boolean
	
	/**
	 * The value of the id, if generated.
	 */
	def value:T
	
	def map[U](f: T=>U):GeneratedId[U]
	def flatMap[U](f: T=>GeneratedId[U]):GeneratedId[U]
}

/**
 * Implementation of [[GeneratedId]] representing a not yet generated id.
 */
case class Transient[T]() extends GeneratedId[T]{
  def isGenerated = false
  /**
   * No value generated.
   * @throws NotImplementedError
   */
  def value:T = throw new NotImplementedError
  def map[U](f: T=>U) = Transient[U]()
  def flatMap[U](f: T=>GeneratedId[U]) = Transient[U]
}

/**
 * Implementation of [[GeneratedId]] representing a generated id.
 */
case class Persistent[T](v:T) extends GeneratedId[T]{
  def isGenerated = true
  def value = v
  def map[U](f: T=>U) = Persistent(f(value))
  def flatMap[U](f: T=>GeneratedId[U]) = f(v)
}