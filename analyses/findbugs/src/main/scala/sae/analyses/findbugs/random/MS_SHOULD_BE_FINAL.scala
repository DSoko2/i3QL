package sae.analyses.findbugs.random

import sae.bytecode.BytecodeDatabase
import idb.Relation
import idb.syntax.iql._
import idb.syntax.iql.IR._


/**
 * @author Ralf Mitschke, Mirko Köhler
 */
object MS_SHOULD_BE_FINAL
	extends (BytecodeDatabase => Relation[BytecodeDatabase#FieldDeclaration])
{

	def apply(database: BytecodeDatabase): Relation[BytecodeDatabase#FieldDeclaration] = {
		import database._

		val ms_fields: Relation[FieldDeclaration] =
			SELECT (*) FROM fieldDeclarations WHERE (
				(f : Rep[FieldDeclaration]) =>
					f.isStatic AND
					NOT (f.isSynthetic) AND
					NOT (f.isVolatile) AND
					(f.isProtected OR f.isPublic) AND
					NOT (f.declaringClass.isInterface)
				)

		SELECT (*) FROM ms_fields WHERE (
			(f : Rep[FieldDeclaration]) =>
				f.isFinal AND
				f.valueType.isDefined AND
				(f.valueType.get.IsInstanceOf[ArrayType[Any]] OR f.valueType.get == ObjectType ("java/util/Hashtable")))

	}

}
