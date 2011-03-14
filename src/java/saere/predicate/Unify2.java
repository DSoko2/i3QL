/* License (BSD Style License):
 * Copyright (c) 2010
 * Department of Computer Science
 * Technische Universität Darmstadt
 * All rights reserved.
 *
 *  Redistribution and use in source and binary forms, with or without
 *  modification, are permitted provided that the following conditions are met:
 *
 *  - Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *  - Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *  - Neither the name of the Software Technology Group or Technische 
 *    Universität Darmstadt nor the names of its contributors may be used to 
 *    endorse or promote products derived from this software without specific 
 *    prior written permission.
 *
 *  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 *  AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 *  IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 *  ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 *  LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 *  CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 *  SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 *  INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 *  CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 *  ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 *  POSSIBILITY OF SUCH DAMAGE.
 */
package saere.predicate;

import saere.PredicateIdentifier;
import saere.PredicateRegistry;
import saere.Goal;
import saere.State;
import saere.StringAtom;
import saere.Term;
import saere.PredicateFactoryTwoArgs;

/**
 * Implementation of ISO Prolog's unify (<code>=/2</code>) operator.
 * 
 * @author Michael Eichberg (mail@michael-eichberg.de)
 */
public final class Unify2 implements Goal {

	public final static PredicateIdentifier IDENTIFIER = new PredicateIdentifier(
			StringAtom.UNIFY, 2);

	public final static PredicateFactoryTwoArgs FACTORY = new PredicateFactoryTwoArgs() {

		@Override
		public Goal createInstance(Term t1, Term t2) {
			return new Unify2(t1, t2);
		}

	};

	public static void registerWithPredicateRegistry(PredicateRegistry registry) {
		registry.register(IDENTIFIER, FACTORY);
	}

	private final Term l;
	private final Term r;
	private State lState;
	private State rState;

	private boolean called = false;

	public Unify2(final Term l, final Term r) {
		this.l = l;
		this.r = r;
	}

	@Override
	public boolean next() {
		if (!called) {
			lState = l.manifestState();
			rState = r.manifestState();
			if (l.unify(r)) {
				called = true;
				return true;
			}
		}
		// unification failed...
		resetTerms();
		return false;
	}

	@Override
	public void abort() {
		resetTerms();
	}

	private void resetTerms() {
		if (lState != null) {
			lState.reincarnate();
			lState = null;
		}
		if (rState != null) {
			rState.reincarnate();
			rState = null;
		}
	}

	@Override
	public boolean choiceCommitted() {
		return false;
	}
}