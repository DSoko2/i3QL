package unisson.ast

/**
 * 
 * Author: Ralf Mitschke
 * Created: 30.08.11 15:00
 *
 */

case class NotAllowedConstraint(source: Ensemble, target: Ensemble, kind: String)
    extends DependencyConstraint
{

}