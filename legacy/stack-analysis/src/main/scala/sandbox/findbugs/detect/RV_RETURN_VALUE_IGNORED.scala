package sandbox.findbugs.detect

import sandbox.stackAnalysis.datastructure.{LocalVariables, Stack, State}
import sandbox.findbugs.BugType
import de.tud.cs.st.bat.resolved._
import sae.bytecode.structure.CodeInfo


/**
 * Created with IntelliJ IDEA.
 * User: Mirko
 * Date: 13.12.12
 * Time: 13:49
 * To change this template use File | Settings | File Templates.
 */
object RV_RETURN_VALUE_IGNORED extends Detector {

  def getDetectorFunction(instr: Instruction): (Int, Instruction, Stack, LocalVariables) => Option[BugType.Value] = {

    if (instr.isInstanceOf[POP.type] || instr.isInstanceOf[POP2.type]) {
      return checkReturnValueIgnored
    }

    return checkNone

  }

  private def checkReturnValueIgnored(pc: Int, instr: Instruction, stack: Stack, lv: LocalVariables): Option[BugType.Value] = {

    if (stack.size > 0 && (stack.get(0).isReturnValue || stack.get(0).isCreatedByNew))
      return Some(BugType.RV_RETURN_VALUE_IGNORED)

    return None
  }
}