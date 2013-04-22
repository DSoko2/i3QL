package sandbox.ast.sql

/**
 *
 * @author: Ralf Mitschke
 */
class FromClause[Domain: Manifest] (table: Table[Domain])
{

  val ir = sandbox.ast.sql.ir

  def SELECT[Range: Manifest] (fun: ir.Rep[Domain] => ir.Rep[Range]): SelectClause[Domain, Range] =
    new SelectClause[Domain, Range](fun)

}
