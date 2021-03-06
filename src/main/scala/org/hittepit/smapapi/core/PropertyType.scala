package org.hittepit.smapapi.core

import java.sql.Blob
import java.sql.Clob
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Types
import Sql._
import org.hittepit.smapapi.core.exception.NullValueException

/**
 * Regroupe les méthodes de base pour accéder aux colonnes d'un ResultSet ou d'écrire les paramètres d'un PreparedStatement
 */
object Sql {
  /** Type pour la définition de méthodes permettant de récupérer un valeur dans un ResultSet */
  type ColumnGetter[T] = ((ResultSet, Int) => T, (ResultSet, String) => T)
  /** Type pour la définition de méthodes permettant d'injecter une valeur dans un PreparedStatement */
  type ColumnSetter[T] = ((PreparedStatement, Int, T) => Unit, Int)

  /**
   * Méthode permettant la récupération d'une valeur dans une colonne Nullable de ResultSet
   * @param getter une méthode permettant de récupérer une valeur dans le ResultSet
   * @param rs le ResultSet
   * @param column soit le nom de la colonne, soit son index
   * @return une option contenant la valeur de la colonne ou valant None si la colonne est null
   */
  def getNullableColumn[T](getter: ColumnGetter[T])(rs: ResultSet, column: Either[String, Int]): Option[T] = {
    val v = column match {
      case Left(c) => getter._2(rs, c)
      case Right(c) => getter._1(rs, c)
    }
    if (rs.wasNull()) None else Some(v)
  }

  /**
   * Méthode permettant la récupération d'une valeur dans une colonne non Nullable de ResultSet
   * @param getter une méthode permettant de récupérer une valeur dans le ResultSet
   * @param rs le ResultSet
   * @param column soit le nom de la colonne, soit son index
   * @return la valeur de la colonne
   * @throws si la colonne est nulle, [[org.hittepit.smapapi.core.exception.NullValueException NullValueException]]
   */
  def getNotNullableColumn[T](getter: ColumnGetter[T])(rs: ResultSet, column: Either[String, Int]): T = {
    val v = column match {
      case Left(c) => getter._2(rs, c)
      case Right(c) => getter._1(rs, c)
    }
    if (rs.wasNull()) throw new NullValueException("Null value in column not expected. Try to use an Optional PropertyType instead.") else v
  }

  /**
   * Méthode permettant d'injecter une valeur optionnelle dans un PreparedStatement
   * @param setter méthode générique permettant l'injection d'une valeur dans un PreparedStatement
   * @param index l'index du paramètre dans le PreparedStatement
   * @param value la valeur du paramètre. Si elle vaut None, null sera injecté comme paramètre
   * @param ps le PreparedStatement visé
   */
  def setNullableColumn[T](setter: ColumnSetter[T])(index: Int, value: Option[T], ps: PreparedStatement) = value match {
    case Some(v) => setter._1(ps, index, v)
    case None => ps.setNull(index, setter._2)
  }

  /**
   * Méthode permettant d'injecter une valeur dans un PreparedStatement
   * @param setter méthode générique permettant l'injection d'une valeur dans un PreparedStatement
   * @param index l'index du paramètre dans le PreparedStatement
   * @param value la valeur du paramètre
   * @param ps le PreparedStatement visé
   */
  def setNotNullableColumn[T](setter: ColumnSetter[T])(index: Int, value: T, ps: PreparedStatement) =
    setter._1(ps, index, value)

  //TODO Float, Date, Time
  val getStringColumn: ColumnGetter[String] = ((rs: ResultSet, index: Int) => rs.getString(index), (rs: ResultSet, name: String) => rs.getString(name))
  val setStringColumn: ColumnSetter[String] = ((ps: PreparedStatement, index: Int, v: String) => ps.setString(index, v), Types.VARCHAR)
  val getClobColumn: ColumnGetter[Clob] = ((rs: ResultSet, index: Int) => rs.getClob(index), (rs: ResultSet, name: String) => rs.getClob(name))
  val setClobColumn: ColumnSetter[Clob] = ((ps: PreparedStatement, index: Int, v: Clob) => ps.setClob(index, v), Types.CLOB)
  val getBlobColumn: ColumnGetter[Blob] = ((rs: ResultSet, index: Int) => rs.getBlob(index), (rs: ResultSet, name: String) => rs.getBlob(name))
  val setBlobColumn: ColumnSetter[Blob] = ((ps: PreparedStatement, index: Int, v: Blob) => ps.setBlob(index, v), Types.BLOB)
  val getIntColumn: ColumnGetter[Int] = ((rs: ResultSet, index: Int) => rs.getInt(index), (rs: ResultSet, name: String) => rs.getInt(name))
  val setIntColumn: ColumnSetter[Int] = ((ps: PreparedStatement, index: Int, v: Int) => ps.setInt(index, v), Types.INTEGER)
  val getLongColumn: ColumnGetter[Long] = ((rs: ResultSet, index: Int) => rs.getLong(index), (rs: ResultSet, name: String) => rs.getLong(name))
  val setLongColumn: ColumnSetter[Long] = ((ps: PreparedStatement, index: Int, v: Long) => ps.setLong(index, v), Types.BIGINT)
  val getDoubleColumn: ColumnGetter[Double] = ((rs: ResultSet, index: Int) => rs.getDouble(index), (rs: ResultSet, name: String) => rs.getDouble(name))
  val setDoubleColumn: ColumnSetter[Double] = ((ps: PreparedStatement, index: Int, v: Double) => ps.setDouble(index, v), Types.DOUBLE)
  val getBooleanColumn: ColumnGetter[Boolean] = ((rs: ResultSet, index: Int) => rs.getBoolean(index), (rs: ResultSet, name: String) => rs.getBoolean(name))
  val setBooleanColumn: ColumnSetter[Boolean] = ((ps: PreparedStatement, index: Int, v: Boolean) => ps.setBoolean(index, v), Types.BOOLEAN)
  val getBigDecimalColumn: ColumnGetter[BigDecimal] = ((rs: ResultSet, index: Int) => rs.getBigDecimal(index), (rs: ResultSet, name: String) => rs.getBigDecimal(name))
  val setBigDecimalColumn: ColumnSetter[BigDecimal] = ((ps: PreparedStatement, index: Int, v: BigDecimal) => ps.setBigDecimal(index, v.bigDecimal), Types.DECIMAL)

}

/**
 * Définition de base d'un type de propriété. Cette classe peut être étendue pour définir n'importe quel type de propriété.
 * 
 * Elle est utilisée pour définir des [[org.hittepit.smapapi.core.session.Param Param]] 
 * injectés dans les PreparedStatement et des [[org.hittepit.smapapi.core.session.Column Column]] pour récupérer des données
 * dans les ResultSet.
 * 
 *  Elle fournit des méthodes qui masquent l'utilisation des ResultSet et des PreparedStatement. 
 */
trait PropertyType[T] {
  /**
   * Méthode qui retourne la valeur de type T d'une colonne définie soit par son index, soit par son nom.
   * @param rs le ResulSet dans lequel la propriété est récupérée
   * @param column la colonne à récupérer dans le ResultSet, soit par son index, soit par son nom
   * @return la valeur de la propriété
   */
  def getColumnValue(rs: ResultSet, column: Either[String, Int]): T
  
  /**
   * Méthode qui permet d'injecter une valeur dans un PreparedStatement.
   * @param index l'index du paramètre dans le PreparedStatement
   * @param value la valeur de type T
   * @param ps le PreparedStatement
   */
  def setColumnValue(index: Int, value: T, ps: PreparedStatement): Unit
}

/**
 * PropertyType pour une propriété de type [[org.hittepit.smapapi.core.GeneratedId GeneratedId[Int]]]
 */
object GeneratedIntId extends PropertyType[GeneratedId[Int]]{
  def getColumnValue(rs: ResultSet, column: Either[String, Int]):GeneratedId[Int] = Persistent(getNotNullableColumn(getIntColumn)(rs, column))
  def setColumnValue(index: Int, value: GeneratedId[Int], ps: PreparedStatement) = {
    val optionalValue = value match {
      case Transient() => None
      case Persistent(v) => Some(v)
    }
    setNullableColumn(setIntColumn)(index, optionalValue, ps)
  }
}

/**
 * PropertyType pour une propriété de type [[org.hittepit.smapapi.core.GeneratedId GeneratedId[Long]]]
 */
object GeneratedLongId extends PropertyType[GeneratedId[Long]]{
  def getColumnValue(rs: ResultSet, column: Either[String, Int]):GeneratedId[Long] = Persistent(getNotNullableColumn(getLongColumn)(rs, column))
  def setColumnValue(index: Int, value: GeneratedId[Long], ps: PreparedStatement) = {
    val optionalValue = value match {
      case Transient() => None
      case Persistent(v) => Some(v)
    }
    setNullableColumn(setLongColumn)(index, optionalValue, ps)
  }
}

/**
 * PropertyType pour une propriété de type Option[String]
 */
object OptionalStringProperty extends PropertyType[Option[String]] {
  def getColumnValue(rs: ResultSet, column: Either[String, Int]) = getNullableColumn(getStringColumn)(rs, column)
  def setColumnValue(index: Int, value: Option[String], ps: PreparedStatement) = setNullableColumn(setStringColumn)(index, value, ps)
}

/**
 * PropertyType pour une propriété de type String
 */
object StringProperty extends PropertyType[String] {
  def getColumnValue(rs: ResultSet, column: Either[String, Int]) = getNotNullableColumn(getStringColumn)(rs, column)
  def setColumnValue(index: Int, value: String, ps: PreparedStatement) = setNotNullableColumn(setStringColumn)(index, value, ps)
}

/**
 * PropertyType pour une propriété de type Option[Clob]
 */
object OptionalClobProperty extends PropertyType[Option[Clob]] {
  def getColumnValue(rs: ResultSet, column: Either[String, Int]) = getNullableColumn(getClobColumn)(rs, column)
  def setColumnValue(index: Int, value: Option[Clob], ps: PreparedStatement) = setNullableColumn(setClobColumn)(index, value, ps)
}

/**
 * PropertyType pour une propriété de type Clob
 */
object ClobProperty extends PropertyType[Clob] {
  def getColumnValue(rs: ResultSet, column: Either[String, Int]) = getNotNullableColumn(getClobColumn)(rs, column)
  def setColumnValue(index: Int, value: Clob, ps: PreparedStatement) = setNotNullableColumn(setClobColumn)(index, value, ps)
}

/**
 * PropertyType pour une propriété de type Option[Blob]
 */
object OptionalBlobProperty extends PropertyType[Option[Blob]] {
  def getColumnValue(rs: ResultSet, column: Either[String, Int]) = getNullableColumn(getBlobColumn)(rs, column)
  def setColumnValue(index: Int, value: Option[Blob], ps: PreparedStatement) = setNullableColumn(setBlobColumn)(index, value, ps)
}

/**
 * PropertyType pour une propriété de type Blob
 */
object BlobProperty extends PropertyType[Blob] {
  def getColumnValue(rs: ResultSet, column: Either[String, Int]) = getNotNullableColumn(getBlobColumn)(rs, column)
  def setColumnValue(index: Int, value: Blob, ps: PreparedStatement) = setNotNullableColumn(setBlobColumn)(index, value, ps)
}

/**
 * PropertyType pour une propriété de type Option[Int]
 */
object OptionalIntProperty extends PropertyType[Option[Int]] {
  def getColumnValue(rs: ResultSet, column: Either[String, Int]) = getNullableColumn(getIntColumn)(rs, column)
  def setColumnValue(index: Int, value: Option[Int], ps: PreparedStatement) = setNullableColumn(setIntColumn)(index, value, ps)
}

/**
 * PropertyType pour une propriété de type Int
 */
object IntProperty extends PropertyType[Int] {
  def getColumnValue(rs: ResultSet, column: Either[String, Int]) = getNotNullableColumn(getIntColumn)(rs, column)
  def setColumnValue(index: Int, value: Int, ps: PreparedStatement) = setNotNullableColumn(setIntColumn)(index, value, ps)
}

/**
 * PropertyType pour une propriété de type Option[Long]
 */
object OptionalLongProperty extends PropertyType[Option[Long]] {
  def getColumnValue(rs: ResultSet, column: Either[String, Int]) = getNullableColumn(getLongColumn)(rs, column)
  def setColumnValue(index: Int, value: Option[Long], ps: PreparedStatement) = setNullableColumn(setLongColumn)(index, value, ps)
}

/**
 * PropertyType pour une propriété de type Long
 */
object LongProperty extends PropertyType[Long] {
  def getColumnValue(rs: ResultSet, column: Either[String, Int]) = getNotNullableColumn(getLongColumn)(rs, column)
  def setColumnValue(index: Int, value: Long, ps: PreparedStatement) = setNotNullableColumn(setLongColumn)(index, value, ps)
}

/**
 * PropertyType pour une propriété de type Option[Double]
 */
object OptionalDoubleProperty extends PropertyType[Option[Double]] {
  def getColumnValue(rs: ResultSet, column: Either[String, Int]) = getNullableColumn(getDoubleColumn)(rs, column)
  def setColumnValue(index: Int, value: Option[Double], ps: PreparedStatement) = setNullableColumn(setDoubleColumn)(index, value, ps)
}

/**
 * PropertyType pour une propriété de type Double
 */
object DoubleProperty extends PropertyType[Double] {
  def getColumnValue(rs: ResultSet, column: Either[String, Int]) = getNotNullableColumn(getDoubleColumn)(rs, column)
  def setColumnValue(index: Int, value: Double, ps: PreparedStatement) = setNotNullableColumn(setDoubleColumn)(index, value, ps)
}

/**
 * PropertyType pour une propriété de type Option[Boolean]
 */
object OptionalBooleanProperty extends PropertyType[Option[Boolean]] {
  def getColumnValue(rs: ResultSet, column: Either[String, Int]) = getNullableColumn(getBooleanColumn)(rs, column)
  def setColumnValue(index: Int, value: Option[Boolean], ps: PreparedStatement) = setNullableColumn(setBooleanColumn)(index, value, ps)
}

/**
 * PropertyType pour une propriété de type Boolean
 */
object BooleanProperty extends PropertyType[Boolean] {
  def getColumnValue(rs: ResultSet, column: Either[String, Int]) = getNotNullableColumn(getBooleanColumn)(rs, column)
  def setColumnValue(index: Int, value: Boolean, ps: PreparedStatement) = setNotNullableColumn(setBooleanColumn)(index, value, ps)
}

/**
 * PropertyType pour une propriété de type Option[BigDecimal]
 */
object OptionalBigDecimalProperty extends PropertyType[Option[BigDecimal]] {
  def getColumnValue(rs: ResultSet, column: Either[String, Int]) = getNullableColumn(getBigDecimalColumn)(rs, column)
  def setColumnValue(index: Int, value: Option[BigDecimal], ps: PreparedStatement) = setNullableColumn(setBigDecimalColumn)(index, value, ps)
}

/**
 * PropertyType pour une propriété de type BigDecimal
 */
object BigDecimalProperty extends PropertyType[BigDecimal] {
  def getColumnValue(rs: ResultSet, column: Either[String, Int]) = getNotNullableColumn(getBigDecimalColumn)(rs, column)
  def setColumnValue(index: Int, value: BigDecimal, ps: PreparedStatement) = setNotNullableColumn(setBigDecimalColumn)(index, value, ps)
}
