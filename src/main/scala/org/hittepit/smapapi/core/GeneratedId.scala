package org.hittepit.smapapi.core

sealed abstract class GeneratedId[T] {
	def isGenerated:Boolean
	def value:T
	
	def map[U](f: T=>U):GeneratedId[U]
	def flatMap[U](f: T=>GeneratedId[U]):GeneratedId[U]
}

case class Transient[T]() extends GeneratedId[T]{
  def isGenerated = false
  def value = throw new NotImplementedError
  def map[U](f: T=>U) = Transient[U]()
  def flatMap[U](f: T=>GeneratedId[U]) = Transient[U]
}

case class Persistent[T](v:T) extends GeneratedId[T]{
  def isGenerated = true
  def value = v
  def map[U](f: T=>U) = Persistent(f(value))
  def flatMap[U](f: T=>GeneratedId[U]) = f(v)
}