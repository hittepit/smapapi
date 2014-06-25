package org.hittepit.smapapi.core.exception

/**
 * Lancée lorsqu'une valeur non nulle est attendue, mais que la valeur (dans la colonne du ResultSet) est nulle.
 * 
 * Elle survient dans les [[org.hittepit.smapapi.core.PropertyType PropertyType]] non optionnels. la solution consiste à utiliser un type optionnel ou à créer
 * son propre [[org.hittepit.smapapi.core.PropertyType PropertyType]].
 */
class NullValueException(msg:String) extends RuntimeException(msg)

/**
 * Lancée lorsqu'un résultat unique est attendu, mais que plusieurs résultats sont renvoyés.
 */
class NotUniqueResultException(msg:String) extends RuntimeException(msg)

