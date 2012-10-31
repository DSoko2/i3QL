package sae
package bytecode
package transform

import sae.bytecode.model._
import dependencies._
import internal.{unresolved_inner_class_entry, unresolved_enclosing_method}
import sae.bytecode.model.instructions._
import de.tud.cs.st.bat._
import de.tud.cs.st.bat.instructions._
import sae.reader.BytecodeFactProcessor

/**
 * Transform a classfile to the SAE representation
 * This implementation delivers the following guarantees
 * Every class and method is distinct on the object level.
 * Thus no two objects with the same values exist.
 * Types are currently NOT distinct
 *
 * A transformer receives a set of methods that are called on each new element.
 * Thus the transformer can be configured to add/delete or update elements or perform
 * altogether different operations
 *
 * There is a tradeoff between having distinct classes/methods and
 * looking concrete objects up during construction.
 * Each lookup costs construction time < vs. > each object costs memory
 * Furthermore subsequent queries could make object referential equality checks for better performance.
 */
class Java6ClassTransformer(
                                   process_class_declaration: ClassDeclaration => Unit,
                                   process_classfile_method: MethodDeclaration => Unit,
                                   process_classfile_field: FieldDeclaration => Unit,
                                   process_class: ObjectType => Unit,
                                   process_method: MethodReference => Unit,
                                   process_field: FieldReference => Unit,
                                   process_instruction: Instr[_] => Unit,
                                   process_extends: `extends` => Unit,
                                   process_implements: implements => Unit,
                                   process_parameter: parameter => Unit,
                                   process_exception_handler: ExceptionHandler => Unit,
                                   process_thrown_exception: throws => Unit,
                                   process_inner_class_entry: unresolved_inner_class_entry => Unit,
                                   process_enclosing_method: unresolved_enclosing_method => Unit
                                   )
        extends TransformInstruction[Unit, MethodDeclaration] with
        BytecodeFactProcessor
{


    private var list: List[ClassFile] = List()

    def processClassFile(cf: de.tud.cs.st.bat.ClassFile) {
        list = cf :: list
    }

    def processAllFacts() {
        // do nothing we need no extra processing of added process_class_declaration
        // but transform the directly
        // here we could schedule parallelization

        list.foreach(transform)

    }

    private def getMethod(typ: Type, name: String, parameters: Seq[de.tud.cs.st.bat.Type],
                          returnType: de.tud.cs.st.bat.Type): MethodReference = {
        if (typ.isObjectType)
            getMethod(typ.asObjectType, name, parameters, returnType)
        else
            getMethod(typ.asArrayType, name, parameters, returnType)

    }

    private def getMethod(declaringRef: ReferenceType, name: String, parameters: Seq[de.tud.cs.st.bat.Type],
                          returnType: de.tud.cs.st.bat.Type): MethodReference = {
        MethodReference(declaringRef, name, parameters, returnType)
    }


    private def getField(declaringClass: ObjectType, name: String, fieldType: FieldType): FieldReference = {
        FieldReference(declaringClass, name, fieldType)
    }

    /**
     * Conversions from constant value to a type
     */
    private def constant_value_function(constantValue: ConstantValue[_]): () => _ =
        constantValue.constant_Pool_EntryType match {
            // Note : hand coded magic numbers elicit a table switch
            case /* CONSTANT_Utf8 */ 1 => () => constantValue.toUTF8
            case /* CONSTANT_Integer */ 3 => () => constantValue.toInt
            case /* CONSTANT_Float */ 4 => () => constantValue.toFloat
            case /* CONSTANT_Long */ 5 => () => constantValue.toLong
            case /* CONSTANT_Double */ 6 => () => constantValue.toDouble
            case /* CONSTANT_Class */ 7 => () => constantValue.toClass
        }


    private def lastInstructionIndex(bytecodeMap: Array[Int]): Int = {
        var i = bytecodeMap.length - 1;
        while (i > 0) {
            if (bytecodeMap(i) != 0) {
                return bytecodeMap(i)
            }
            i = i - 1;
        }
        0;
    }

    /**
     *
     */
    private def transform(classFile: de.tud.cs.st.bat.ClassFile) {
        // these process_class are always unique, no check is performed
        process_class(classFile.thisClass)

        process_class_declaration(ClassDeclaration(classFile.thisClass, classFile.accessFlags, classFile
                .isDeprecated, classFile.isSynthetic))

        // Note: there is exactly one class (java/lang/Object) that has no superclass
        if (classFile.superClass != null) {
            `process_extends`(new `extends`(classFile.thisClass, classFile.superClass))
        }
        classFile.interfaces.foreach(i =>
            process_implements(new implements(classFile.thisClass, i))
        )

        classFile.methods.foreach(transform(classFile.thisClass, _))

        classFile.fields.foreach(transform(classFile.thisClass, _))

        classFile.attributes.foreach {
            case ica: InnerClasses_attribute => transform(classFile.thisClass, ica)
            case ema: EnclosingMethod_attribute => transform(classFile.thisClass, ema)
            case _ => // do nothing
        }
    }

    /**
     *
     */
    private def transform(declaringClass: ObjectType, method_info: Method_Info) {
        val method = MethodDeclaration(declaringClass,
            method_info.name,
            method_info.descriptor.parameterTypes,
            method_info.descriptor.returnType,
            method_info.accessFlags,
            method_info.isDeprecated,
            method_info.isSynthetic
        )

        process_classfile_method(method);

        method_info.descriptor.parameterTypes.foreach(p =>
            (
                    process_parameter(new parameter(method, p)))
        )

        method_info.attributes.foreach(
        {
            case code_attribute: Code_attribute => transform(method, code_attribute)
            case exceptions_attribute: Exceptions_attribute => transform(method, exceptions_attribute)
            case _ => // do nothing for currently unsupported attributes
        })
    }


    /**
     *
     */
    private def transform(declaringClass: ObjectType, field_info: Field_Info) {
        val field = FieldDeclaration(declaringClass,
            field_info.name,
            field_info.descriptor.fieldType,
            field_info.accessFlags,
            field_info.isDeprecated,
            field_info.isSynthetic)
        process_classfile_field(field);
    }

    /**
     * The InnerClasses attribute5 is a variable-length attribute in the attributes table
     * of the ClassFile (§4.2) structure. If the constant pool of a class or interface C contains
     * a CONSTANT_Class_info entry which represents a class or interface that is
     * not a member of a package, then C‘s ClassFile structure must have exactly one
     * InnerClasses attribute in its attributes table.
     *
     * If C is a member, the value of the outer_class_info_index item is not zero.
     *
     * If C is anonymous, the value of the inner_name_index item must be zero.
     */
    private def transform(declaringClass: ObjectType, innerClasses: InnerClasses_attribute) {
        innerClasses.classes
                .foreach((e: InnerClassesEntry) =>
            process_inner_class_entry(unresolved_inner_class_entry(declaringClass, e
                    .innerClassType, e.outerClassType, e.innerName, e.accessFlags)))
    }

    /**
     * The EnclosingMethod attribute is an optional fixed-length attribute in the attributes table of the ClassFile (§4.2) structure.
     * A class must have an EnclosingMethod attribute if and only if it is a local class or an anonymous class.
     * A class may have no more than one EnclosingMethod attribute.
     */
    private def transform(declaringClass: ObjectType, enclosingMethod: EnclosingMethod_attribute) {
        // val source = getMethod(enclosingMethod.clazz, enclosingMethod.name, enclosingMethod.descriptor.parameterTypes, enclosingMethod.descriptor.returnType)
        process_enclosing_method(new unresolved_enclosing_method(
            enclosingMethod.clazz,
            if (enclosingMethod.name eq null) {
                None
            } else {
                Some(enclosingMethod.name)
            },
            if (enclosingMethod.descriptor eq null) {
                None
            } else {
                Some(enclosingMethod.descriptor.parameterTypes)
            },
            if (enclosingMethod.descriptor eq null) {
                None
            } else {
                Some(enclosingMethod.descriptor.returnType)
            },
            declaringClass)
        )
    }

    private def transform(declaringMethod: MethodDeclaration, exceptions_attribute: Exceptions_attribute) {
        exceptions_attribute.exceptionTable.foreach(e => process_thrown_exception(new throws(declaringMethod, e)))
    }

    /**
     * transform the individual bytecode instructions
     */
    private def transform(declaringMethod: MethodDeclaration, code_attribute: Code_attribute) {
        var pc = 0

        code_attribute.exceptionTable.foreach(entry => {
            val handler = new ExceptionHandler(
                declaringMethod,
                (
                        if (entry.catchType == null)
                            None
                        else
                            Some(entry.catchType)
                        ),
                entry.startPC,
                (
                        if (entry.endPC >= code_attribute.bytecodeMap.length)
                            lastInstructionIndex(code_attribute.bytecodeMap)
                        else
                            code_attribute.bytecodeMap(entry.endPC)
                        ),
                entry.handlerPC
            )
            process_exception_handler(handler)
        }
        )

        code_attribute.code.foreach(instr => {
            transform(instr, pc, code_attribute.bytecodeMap, declaringMethod)
            pc += 1
        }
        )
    }

    def transform_instruction_default(instr: Instruction, pc: Int, declaringMethod: MethodDeclaration) {
        // do nothing for process_instruction that we don't want to support yet
    }

    override def transform_BAT_invokeinterface(instr: BAT_invokeinterface, pc: Int, bytecodeMap: Array[Int],
                                               declaringMethod: MethodDeclaration) {
        val callee = getMethod(instr.declaring_class_type, instr.method_name.toUTF8, instr.method_parameters
                .asFieldTypeSeq, instr.method_return_type)
        val instruction = invokeinterface(declaringMethod, pc, callee)
        process_instruction(instruction)
    }

    override def transform_BAT_invokespecial(instr: BAT_invokespecial, pc: Int, bytecodeMap: Array[Int],
                                             declaringMethod: MethodDeclaration) {
        val callee = getMethod(instr.declaring_class_type, instr.method_name.toUTF8, instr.method_parameters
                .asFieldTypeSeq, instr.method_return_type)
        val instruction = invokespecial(declaringMethod, pc, callee)
        process_instruction(instruction)
    }

    override def transform_BAT_invokestatic(instr: BAT_invokestatic, pc: Int, bytecodeMap: Array[Int],
                                            declaringMethod: MethodDeclaration) {
        val callee = getMethod(instr.declaring_class_type, instr.method_name.toUTF8, instr.method_parameters
                .asFieldTypeSeq, instr.method_return_type)
        val instruction = invokestatic(declaringMethod, pc, callee)
        process_instruction(instruction)
    }

    override def transform_BAT_invokevirtual(instr: BAT_invokevirtual, pc: Int, bytecodeMap: Array[Int],
                                             declaringMethod: MethodDeclaration) {
        val callee = getMethod(instr.declaring_class_type, instr.method_name.toUTF8, instr.method_parameters
                .asFieldTypeSeq, instr.method_return_type)
        val instruction = invokevirtual(declaringMethod, pc, callee)
        process_instruction(instruction)
    }

    override def transform_BAT_putstatic(instr: BAT_putstatic, pc: Int, bytecodeMap: Array[Int],
                                         declaringMethod: MethodDeclaration) {
        val field = getField(instr.declaringClass.asObjectType, instr.fieldName.toUTF8, instr.fieldType.asFieldType)
        val instruction = putstatic(declaringMethod, pc, field)
        process_instruction(instruction)
    }

    override def transform_BAT_putfield(instr: BAT_putfield, pc: Int, bytecodeMap: Array[Int],
                                        declaringMethod: MethodDeclaration) {
        val field = getField(instr.declaringClass.asObjectType, instr.fieldName.toUTF8, instr.fieldType.asFieldType)
        val instruction = putfield(declaringMethod, pc, field)
        process_instruction(instruction)
    }

    override def transform_BAT_getstatic(instr: BAT_getstatic, pc: Int, bytecodeMap: Array[Int],
                                         declaringMethod: MethodDeclaration) {
        val field = getField(instr.declaringClass.asObjectType, instr.fieldName.toUTF8, instr.fieldType.asFieldType)
        val instruction = getstatic(declaringMethod, pc, field)
        process_instruction(instruction)
    }

    override def transform_BAT_getfield(instr: BAT_getfield, pc: Int, bytecodeMap: Array[Int],
                                        declaringMethod: MethodDeclaration) {
        val field = getField(instr.declaringClass.asObjectType, instr.fieldName.toUTF8, instr.fieldType.asFieldType)
        val instruction = getfield(declaringMethod, pc, field)
        process_instruction(instruction)
    }

    override def transform_BAT_checkcast(instr: BAT_checkcast, pc: Int, bytecodeMap: Array[Int],
                                         declaringMethod: MethodDeclaration) {
        val instruction = checkcast(declaringMethod, pc, instr.T.asReferenceType)
        process_instruction(instruction)
    }

    override def transform_BAT_newarray(instr: BAT_newarray, pc: Int, bytecodeMap: Array[Int],
                                        declaringMethod: MethodDeclaration) {
        val instruction = newarray(declaringMethod, pc, instr.T)
        process_instruction(instruction)
    }

    override def transform_BAT_new(instr: BAT_new, pc: Int, bytecodeMap: Array[Int],
                                   declaringMethod: MethodDeclaration) {
        val instruction = `new`(declaringMethod, pc, instr.T.asObjectType)
        process_instruction(instruction)
    }

    override def transform_BAT_instanceof(instr: BAT_instanceof, pc: Int, bytecodeMap: Array[Int],
                                          declaringMethod: MethodDeclaration) {
        val instruction = sae.bytecode.model.instructions.instanceof(declaringMethod, pc, instr.T.asReferenceType)
        process_instruction(instruction)
    }

    override def transform_BAT_cast(instr: BAT_cast, pc: Int, bytecodeMap: Array[Int],
                                    declaringMethod: MethodDeclaration) {
        val instruction = cast(declaringMethod, pc, instr.S, instr.T)
        process_instruction(instruction)
    }

    override def transform_BAT_push(instr: BAT_push, pc: Int, bytecodeMap: Array[Int],
                                    declaringMethod: MethodDeclaration) {
        def createPush[T](f: () => T) = new push[T](declaringMethod, pc, f(), instr.T)
        val instruction = createPush(constant_value_function(instr.value))
        process_instruction(instruction)
    }


}