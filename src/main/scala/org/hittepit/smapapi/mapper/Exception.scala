package org.hittepit.smapapi.mapper

case class NullValueException() extends RuntimeException

case class ConfigurationException(msg:String) extends RuntimeException(msg)