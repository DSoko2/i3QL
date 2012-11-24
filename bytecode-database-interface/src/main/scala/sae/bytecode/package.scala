package sae

import bytecode.instructions.{InstructionInfo, InvokeInstruction}
import bytecode.structure._
import bytecode.structure.MethodDeclaration
import de.tud.cs.st.bat.resolved.{ObjectType, Type, ReferenceType, VoidType}

/**
 * Created with IntelliJ IDEA.
 * User: Ralf Mitschke
 * Date: 09.08.12
 * Time: 22:40
 */

package object bytecode
{

    type ClassType = de.tud.cs.st.bat.resolved.ObjectType

    def ClassType(fullyQualified: String): ClassType = de.tud.cs.st.bat.resolved.ObjectType (fullyQualified)

    val void = VoidType


    def declaringClass: DeclaredClassMember => ClassDeclaration = _.declaringClass

    def declaringType: DeclaredClassMember => ClassType = _.declaringClass.classType

    def declaringMethod: InstructionInfo => MethodDeclaration = _.declaringMethod

    def sequenceIndex: InstructionInfo => Int = _.sequenceIndex

    def receiverType: InvokeInstruction => ReferenceType = _.receiverType

    def returnType: MethodInfo => Type = _.returnType

    def classType: ClassDeclaration => ClassType = _.classType

    def subType : InheritanceRelation => ClassType = _.subType

    def superType : InheritanceRelation => ClassType = _.superType

    def superClass : ClassDeclaration => ClassType = _.superClass.get

    def declaringClassType : InstructionInfo => ClassType = _.declaringMethod.declaringClass.classType
}
