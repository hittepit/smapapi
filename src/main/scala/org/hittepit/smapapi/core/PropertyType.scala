package org.hittepit.smapapi.core

import java.sql.ResultSet
import java.sql.PreparedStatement
import java.sql.Types
import java.sql.Clob
import java.sql.Blob

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
   * @throws si la colonne est nulle, [[org.hittepit.smapapi.core.NullValueException NullValueException]]
   */
  def getNotNullableColumn[T](getter: ColumnGetter[T])(rs: ResultSet, column: Either[String, Int]): T = {
    val v = column match {
      case Left(c) => getter._2(rs, c)
      case Right(c) => getter._1(rs, c)
    }
    if (rs.wasNull()) throw new NullValueException else v
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

  //TODO Long, Float, Date, Time
  val getStringColumn: ColumnGetter[String] = ((rs: ResultSet, index: Int) => rs.getString(index), (rs: ResultSet, name: String) => rs.getString(name))
  val setStringColumn: ColumnSetter[String] = ((ps: PreparedStatement, index: Int, v: String) => ps.setString(index, v), Types.VARCHAR)
  val getClobColumn: ColumnGetter[Clob] = ((rs: ResultSet, index: Int) => rs.getClob(index), (rs: ResultSet, name: String) => rs.getClob(name))
  val setClobColumn: ColumnSetter[Clob] = ((ps: PreparedStatement, index: Int, v: Clob) => ps.setClob(index, v), Types.CLOB)
  val getBlobColumn: ColumnGetter[Blob] = ((rs: ResultSet, index: Int) => rs.getBlob(index), (rs: ResultSet, name: String) => rs.getBlob(name))
  val setBlobColumn: ColumnSetter[Blob] = ((ps: PreparedStatement, index: Int, v: Blob) => ps.setBlob(index, v), Types.BLOB)
  val getIntColumn: ColumnGetter[Int] = ((rs: ResultSet, index: Int) => rs.getInt(index), (rs: ResultSet, name: String) => rs.getInt(name))
  val setIntColumn: ColumnSetter[Int] = ((ps: PreparedStatement, index: Int, v: Int) => ps.setInt(index, v), Types.INTEGER)
  val getDoubleColumn: ColumnGetter[Double] = ((rs: ResultSet, index: Int) => rs.getDouble(index), (rs: ResultSet, name: String) => rs.getDouble(name))
  val setDoubleColumn: ColumnSetter[Double] = ((ps: PreparedStatement, index: Int, v: Double) => ps.setDouble(index, v), Types.DOUBLE)
  val getBooleanColumn: ColumnGetter[Boolean] = ((rs: ResultSet, index: Int) => rs.getBoolean(index), (rs: ResultSet, name: String) => rs.getBoolean(name))
  val setBooleanColumn: ColumnSetter[Boolean] = ((ps: PreparedStatement, index: Int, v: Boolean) => ps.setBoolean(index, v), Types.BOOLEAN)
  val getBigDecimalColumn: ColumnGetter[BigDecimal] = ((rs: ResultSet, index: Int) => rs.getBigDecimal(index), (rs: ResultSet, name: String) => rs.getBigDecimal(name))
  val setBigDecimalColumn: ColumnSetter[BigDecimal] = ((ps: PreparedStatement, index: Int, v: BigDecimal) => ps.setBigDecimal(index, v.bigDecimal), Types.DECIMAL)

}

import Sql._

object PropertyTypes{
  private var _propertyTypes = Map[String,PropertyType[_]]()
  
  def add[T](propertyClass:Class[T],propertyType:PropertyType[T]) = {
    if(propertyClass == classOf[Some[_]]){
      throw new Exception("Ne fonctionne pas pour des options") //TODO exception
    }
    _propertyTypes += propertyClass.getName() -> propertyType
  }
  
  def propertyType[T](propertyClass:Class[T]) = _propertyTypes.get(propertyClass.getName) match {
    case Some(p) => p.asInstanceOf[PropertyType[T]]
    case None => throw new Exception("Pas de property définie pour cette classe") //TODO exxeption
  } 
}

trait PropertyType[T] {
  def getColumnValue(rs: ResultSet, column: Either[String, Int]): T
  def setColumnValue(index: Int, value: T, ps: PreparedStatement): Unit
}

object NullableString extends PropertyType[Option[String]] {
  def getColumnValue(rs: ResultSet, column: Either[String, Int]) = getNullableColumn(getStringColumn)(rs, column)
  def setColumnValue(index: Int, value: Option[String], ps: PreparedStatement) = setNullableColumn(setStringColumn)(index, value, ps)
}

object NotNullableString extends PropertyType[String] {
  PropertyTypes.add(classOf[String], this)
  def getColumnValue(rs: ResultSet, column: Either[String, Int]) = getNotNullableColumn(getStringColumn)(rs, column)
  def setColumnValue(index: Int, value: String, ps: PreparedStatement) = setNotNullableColumn(setStringColumn)(index, value, ps)
}

object NullableClob extends PropertyType[Option[Clob]] {
  def getColumnValue(rs: ResultSet, column: Either[String, Int]) = getNullableColumn(getClobColumn)(rs, column)
  def setColumnValue(index: Int, value: Option[Clob], ps: PreparedStatement) = setNullableColumn(setClobColumn)(index, value, ps)
}

object NotNullableClob extends PropertyType[Clob] {
  PropertyTypes.add(classOf[Clob], this)
  def getColumnValue(rs: ResultSet, column: Either[String, Int]) = getNotNullableColumn(getClobColumn)(rs, column)
  def setColumnValue(index: Int, value: Clob, ps: PreparedStatement) = setNotNullableColumn(setClobColumn)(index, value, ps)
}

object NullableBlob extends PropertyType[Option[Blob]] {
  def getColumnValue(rs: ResultSet, column: Either[String, Int]) = getNullableColumn(getBlobColumn)(rs, column)
  def setColumnValue(index: Int, value: Option[Blob], ps: PreparedStatement) = setNullableColumn(setBlobColumn)(index, value, ps)
}

object NotNullableBlob extends PropertyType[Blob] {
  PropertyTypes.add(classOf[Blob], this)
  def getColumnValue(rs: ResultSet, column: Either[String, Int]) = getNotNullableColumn(getBlobColumn)(rs, column)
  def setColumnValue(index: Int, value: Blob, ps: PreparedStatement) = setNotNullableColumn(setBlobColumn)(index, value, ps)
}

object NullableInt extends PropertyType[Option[Int]] {
  def getColumnValue(rs: ResultSet, column: Either[String, Int]) = getNullableColumn(getIntColumn)(rs, column)
  def setColumnValue(index: Int, value: Option[Int], ps: PreparedStatement) = setNullableColumn(setIntColumn)(index, value, ps)
}

object NotNullableInt extends PropertyType[Int] {
  PropertyTypes.add(classOf[Int], this)
  PropertyTypes.add(classOf[java.lang.Integer], this.asInstanceOf[PropertyType[Integer]])
  def getColumnValue(rs: ResultSet, column: Either[String, Int]) = getNotNullableColumn(getIntColumn)(rs, column)
  def setColumnValue(index: Int, value: Int, ps: PreparedStatement) = setNotNullableColumn(setIntColumn)(index, value, ps)
}

object NullableDouble extends PropertyType[Option[Double]] {
  def getColumnValue(rs: ResultSet, column: Either[String, Int]) = getNullableColumn(getDoubleColumn)(rs, column)
  def setColumnValue(index: Int, value: Option[Double], ps: PreparedStatement) = setNullableColumn(setDoubleColumn)(index, value, ps)
}

object NotNullableDouble extends PropertyType[Double] {
  PropertyTypes.add(classOf[Double], this)
  def getColumnValue(rs: ResultSet, column: Either[String, Int]) = getNotNullableColumn(getDoubleColumn)(rs, column)
  def setColumnValue(index: Int, value: Double, ps: PreparedStatement) = setNotNullableColumn(setDoubleColumn)(index, value, ps)
}

object NullableBoolean extends PropertyType[Option[Boolean]] {
  def getColumnValue(rs: ResultSet, column: Either[String, Int]) = getNullableColumn(getBooleanColumn)(rs, column)
  def setColumnValue(index: Int, value: Option[Boolean], ps: PreparedStatement) = setNullableColumn(setBooleanColumn)(index, value, ps)
}

object NotNullableBoolean extends PropertyType[Boolean] {
  PropertyTypes.add(classOf[Boolean], this)
  def getColumnValue(rs: ResultSet, column: Either[String, Int]) = getNotNullableColumn(getBooleanColumn)(rs, column)
  def setColumnValue(index: Int, value: Boolean, ps: PreparedStatement) = setNotNullableColumn(setBooleanColumn)(index, value, ps)
}

object NullableBigDecimal extends PropertyType[Option[BigDecimal]] {
  def getColumnValue(rs: ResultSet, column: Either[String, Int]) = getNullableColumn(getBigDecimalColumn)(rs, column)
  def setColumnValue(index: Int, value: Option[BigDecimal], ps: PreparedStatement) = setNullableColumn(setBigDecimalColumn)(index, value, ps)
}

object NotNullableBigDecimal extends PropertyType[BigDecimal] {
  PropertyTypes.add(classOf[BigDecimal], this)
  def getColumnValue(rs: ResultSet, column: Either[String, Int]) = getNotNullableColumn(getBigDecimalColumn)(rs, column)
  def setColumnValue(index: Int, value: BigDecimal, ps: PreparedStatement) = setNotNullableColumn(setBigDecimalColumn)(index, value, ps)
}
