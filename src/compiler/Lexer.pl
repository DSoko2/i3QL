/* License (BSD Style License):
   Copyright (c) 2010
   Department of Computer Science
   Technische Universität Darmstadt
   All rights reserved.

   Redistribution and use in source and binary forms, with or without
   modification, are permitted provided that the following conditions are met:

    - Redistributions of source code must retain the above copyright notice,
      this list of conditions and the following disclaimer.
    - Redistributions in binary form must reproduce the above copyright notice,
      this list of conditions and the following disclaimer in the documentation
      and/or other materials provided with the distribution.
    - Neither the name of the Software Technology Group or Technische 
      Universität Darmstadt nor the names of its contributors may be used to 
      endorse or promote products derived from this software without specific 
      prior written permission.

   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
   AND ANY EXPRESSED OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
   IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
   ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
   LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
   CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
   SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
   INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
   CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
   ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
   POSSIBILITY OF SUCH DAMAGE.
*/




/**
   A lexer to tokenize standard (ISO/SAE) Prolog files.<br />
	We use the terminology as described in the Book "Learn Prolog Now!" which
	is freely available (www.learnprolognow.org).
   <p>
   The result of tokenizing a file is a list of elementary tokens of Prolog
	programs.<br/>
   The types of tokens recognized by this lexer is shown in the following. 
   <code>LN</code> and <code>CN</code> always identify the line and
   column where the token was found.
   <ul>
   <li>a(S,LN,CN) :- S is an atom; basically a string of chars or a sequence of
		operators (e.g., ':-').<br />
		<i>The cut ("!") is also treated as an
      atom as well as the operators that cannot appear as part 
		of an operator sequence (',','|').</i></li>
   <li>f(F,LN,CN) :- F is a functor, i.e., an atom that was immediately 
		followed by '('. However, ',' and '|' are never directly the functor
		of a compound term, they always need to be put between two apostrophs to 
		be used as a functor. This special treatment is necessary to enable 
		"natural" definitions of complex terms and lists.
		</i></li>		
   <li>i(N,LN,CN) :- N is an integer value.</li>      
   <li>r(N,LN,CN) :- N is a real value / a floating point value.</li>      
   <li>v(V,LN,CN) :- V is the name of a concrete variable (v); i.e., V 
      always starts with an upper case letter.</li>
   <li>av(V,LN,CN) :- V is the name of an anonymous variable (av); i.e., V
      always starts with a under score ('_').</li>
   <li>P(LN,CN); P is a paranthesis (e.g., one of "(,),{,},[,]").</li>     
   <li>chars(S,LN,CN) :- S is a list of char atoms. For 
      example, given the string <code>"A test"</code> then the result is the 
      token <code>chars([A,  , t, e, s, t], 1, 0)</code>.
   </ul>
	To include structured comments in the list
   of tokens use the option <code>comments(retain_sc)</code>.
 	<ul>
  	<li>sc(Tokens,LN,CN) :- Tokens of a structured comment (/ * * .. * /).</li>
	</ul>
   The following tokens are only relevant when implementing, e.g., a pretty 
   printer. To include comments in the list
   of tokens use the option <code>comments(retain_all)</code>. 
   <ul>  
   <li>eolc(Token,LN,CN) :- Token is an end-of-line comment (% ...).</li>
   <li>mlc(Token,LN,CN) :- Token is multi-line / inline comment (using / * .. * /).</li>  
   </ul>
   </p>
   <p>
   <b>Example</b>
   Given a file that starts with:<br/>
   <code><br />
   :- module('Lexer',[tokenize_file/2]).<br />
   <code><br />
   then the result of running this lexer is the following token sequence:
   <pre>
      tokenize_file(&lt;THE_FILE&gt;,Ts)
      Ts = [
         a(:-, 1, 0),
         f(module, 1, 3),
         '('(1,8),
         a('Lexer', 1, 10),
         a(',', 1, 17),
         '['(1,18),
         a(tokenize_file, 1, 19),
         a(/, 1, 32),
         i(..., ..., ...)|...].
   </pre>
   </p>  
   <p>
   <blockquote>
   <b>Implementation Note</b>
   This is a hand written lexer that delivers sufficient performance for
   lexing real world (ISO) Prolog programs. The lexer provides information
   about a token's position to enable subsequent phases to give precise error
   messages. If the lexer does not recognize a special character / symbol it 
   prints an error or warning message, but otherwise just ignores the character
   and continues with the next character.<br />
   Conceptually, the lexer is implemented following a state machine model, where
   a character causes the lexer to transition to the next state.
   </p>
   </blockquote>
   
   @author Michael Eichberg (mail@michael-eichberg.de)
   @version 0.9.1 - July, 28th 2010 
		Changed the representation of parantheses, comments and operators.
   @version 0.9 - July, 23th 2010 
		The lexer works, but is not yet fully tested.
*/
:- module(
   'SAEProlog:Compiler:Lexer',
   [
      tokenize_file/2,
      tokenize_file/3,
      tokenize/2,
      tokenize_with_sc/2,
      tokenize_with_c/2,
      operator_char/1,
		parenthesis/1
   ]
).



/**
   Tokenizes a file and returns the list of tokens. White space information is
   stripped.<br />
   For further details see {@link tokenize_file/3}.
*/
tokenize_file(File,Tokens) :- tokenize_file(File,Tokens,[]).

/**
   Tokenizes a file and returns the list of tokens.
   
   @signature tokenize_file(File,Tokens,Options)
   @arg(in) File the file to tokenize.
   @arg(out) Tokens the list of recognized tokens.
   @arg(in) Options a list of options to parameterize the lexer. Currently, 
      the only supported option is <code>comments(Mode)</code>, where Mode
      is either <code>retain_all</code>, <code>retain_sc</code> or <code>drop</code>
      and which determines which type of commens are included in the Tokens 
      list.<br/>
*/
tokenize_file(File,Tokens,Options) :-
   open(File,read,Stream),
   (
      member(comments(Mode),Options),!,
      (
         Mode = retain_all,!,
         tokenize_with_c(Stream,Tokens)        
      ;
         Mode = retain_sc,!,
         tokenize_with_sc(Stream,Tokens)
      ;
         write('Error: unrecognized mode ('),write(Mode),write(')'),nl
      )
   ;  
      tokenize(Stream,Tokens) 
   ),
   close(Stream).



/**
   Tokenizes a stream of characters and retains all comments.<br />
   If no (structured) comments are required consider using {@link tokenize_with_c/2}
   or {@link tokenize/2}.
   
   @signature tokenize(Stream,Tokens)
   @arg(in) Stream a stream that supports reading characters
   @arg(out) Tokens the list of recognized tokens
*/
tokenize_with_c(Stream,Tokens) :-
   stream_position(Stream,LN,CN),
   current_stream(File,read,Stream),
   get_char(Stream,C),
   read_token(C,Stream,T),
   !,
   (  % if...
      T = ws, !,
      Tokens = Ts
   ;  % else...
      token_with_position(T,File,LN,CN,TwithPos),
      Tokens = [TwithPos|Ts]
   ),
   tokenize_with_c(Stream,Ts).
tokenize_with_c(_Stream,[]). % we reached the end of the file


/**
   Tokenizes a stream of characters comments (except of structured comments) are
	dropped.
   
   @signature tokenize(Stream,Tokens)
   @arg(in) Stream a stream that supports reading characters
   @arg(out) Tokens the list of recognized tokens
*/
tokenize_with_sc(Stream,Tokens) :-
   stream_position(Stream,LN,CN),
   current_stream(File,read,Stream),
   get_char(Stream,C),
   read_token(C,Stream,T),
   !,
   (  % if...
      ( is_unstructured_comment(T) ; T=ws ), !,
      Tokens = Ts
   ;  % else...
      token_with_position(T,File,LN,CN,TwithPos),
      Tokens = [TwithPos|Ts]
   ),
   tokenize_with_sc(Stream,Ts).
tokenize_with_sc(_Stream,[]). % we reached the end of the file


/**
   Tokenizes a stream of characters; all comments are dropped.

   @signature tokenize(Stream,Tokens)
   @arg(in) Stream a stream that supports reading characters
   @arg(out) Tokens the list of recognized tokens
*/
tokenize(Stream,Tokens) :-
   stream_position(Stream,LN,CN),
   current_stream(File,read,Stream),
   get_char(Stream,C),
   read_token(C,Stream,T),
   !,
   (  % if...
      is_insignificant(T), !,
      Tokens = Ts
   ;  % else...
      token_with_position(T,File,LN,CN,TwithPos),
      Tokens = [TwithPos|Ts]
   ),
   tokenize(Stream,Ts).
tokenize(_Stream,[]). % we reached the end of the file



/**
   The list of all operators that are allowed to be combined to form new
   operator names, such as, ":-" or "=/=".
*/
operator_char('='). % "=" is not mentioned in the "ISO Prolog" book, but in "Learn Prolog Now"
operator_char('+').
operator_char('-').
operator_char('*').
operator_char('/').
operator_char('\\').
operator_char('~').
operator_char('^').
operator_char('<').
operator_char('>').
operator_char(':').
operator_char('.').
operator_char('?').
operator_char('@').
operator_char('#').
operator_char('$').
operator_char('&').



parenthesis('(').
parenthesis(')').
parenthesis('[').
parenthesis(']').
parenthesis('{').
parenthesis('}').



/******************************************************************************\
 *                                                                            *
 *                P R I V A T E     I M P L E M E N T A T I O N               *
 *                                                                            *
\******************************************************************************/



is_unstructured_comment(eolc(_)) :- !.
is_unstructured_comment(mlc(_)) :- !.

is_structured_comment(sc(_)) :- !.

is_comment(T) :- is_unstructured_comment(T).
is_comment(T) :- is_structured_comment(T).

is_insignificant(ws) :- !.
is_insignificant(T) :- is_comment(T),!.



% standard tokens (relevant when compiling a program)
token_with_position(a(T),File,LN,CN,a(T,pos(File,LN,CN))) :- !.
token_with_position(f(T),File,LN,CN,f(T,pos(File,LN,CN))) :- !.
token_with_position(i(T),File,LN,CN,i(T,pos(File,LN,CN))) :- !.
token_with_position(r(T),File,LN,CN,r(T,pos(File,LN,CN))) :- !.
token_with_position(v(T),File,LN,CN,v(T,pos(File,LN,CN))) :- !.
token_with_position(av(T),File,LN,CN,av(T,pos(File,LN,CN))) :- !.
token_with_position(P,File,LN,CN,PwithPos) :- parenthesis(P),!,PwithPos =.. [P,pos(File,LN,CN)].
token_with_position(chars(T),File,LN,CN,chars(T,pos(File,LN,CN))) :- !.

% comments
token_with_position(sc(T),File,LN,CN,sc(T,pos(File,LN,CN))) :- !.
token_with_position(eolc(T),File,LN,CN,eolc(T,pos(File,LN,CN))) :- !.
token_with_position(mlc(T),File,LN,CN,mlc(T,pos(File,LN,CN))) :- !.

% to help debugging the lexer
token_with_position(T,File,LN,CN,T) :- 
	atomic_list_concat(
		['INTERNAL LEXER ERROR:',File,':',LN,':',CN,' unknown token (\'',T,'\' [FATAL]'],MSG),
	write(MSG), 
	fail.



/**
   Reads in a token of a specific type.</br >
   Based on the previously read character Char and at most one further character 
   (using peek_char), the type of the token is determined and reading the rest 
   of the token's chars is delegated to the corresponding "read_*" predicates.
   
   @signature read_token(Char,Stream,Token)
   @arg(in) Char the last read token. This character is used to identify the 
      type of the current token and determines how the following characters
      are interpreted.
   @arg(out) Token is the read token represented using a compound term describing 
		the token and its position.
*/
read_token('%',Stream,eolc(Token)) :- 
   !,
   read_eol_comment(Stream,Cs),
   atom_chars(Token,Cs).

% parantheses are "returned" as is 
read_token('(',_Stream,'(') :- !.
read_token(')',_Stream,')') :- !.
read_token('{',_Stream,'{') :- !.
read_token('}',_Stream,'}') :- !.
read_token('[',_Stream,'[') :- !.
read_token(']',_Stream,']') :- !.

% the "and (,)" and "|" are treated specially, because they cannot
% contribute to an operator name (e.g. ':-') and they can never be a functor.
read_token(',',_Stream,a(',')) :- !.
read_token('|',_Stream,a('|')) :- !.

% the ';' and the '!' can be functors, but the never contribute to an atom 
% consisting of operator chars
read_token(';',Stream,T) :- !, qualify_as_functor_or_atom(';',Stream,T).
read_token('!',Stream,T) :- !, qualify_as_functor_or_atom('!',Stream,T).

% an anonymous variable
read_token('_',Stream,av(AV)) :- 
   !,
   read_identifier(Stream,Cs),
   atom_chars(AV,['_'|Cs]).

% a quoted atom
read_token('\'',Stream,T) :- 
   !,
   read_string_with_quotations(Stream,'\'',Cs),
   atom_chars(QA,Cs),
 	qualify_as_functor_or_atom(QA,Stream,T).
   
% a string  
read_token('"',Stream,chars(S)) :- 
   !, 
   read_string_with_quotations(Stream,'"',S).

% white space
read_token(C,_Stream,ws) :- 
	char_type(C,space),
	!.

% a variable name
read_token(C,Stream,v(V)) :- 
   char_type(C,upper),
   !,
   read_identifier(Stream,Cs),
   atom_chars(V,[C|Cs]).   

% a number
read_token(I,Stream,R) :-
   char_type(I,digit),
   !,
   read_number(Stream,Is),
   number_chars(N,[I|Is]),
   (  
      integer(N),!,
      R = i(N)
   ;
      R = r(N)
   )
   .

% a multi-line comment; it is either a structured or an unstructured comment
read_token('/',Stream,Token) :- 
   peek_char(Stream,'*'),
   !, 
   get_char(Stream,_), % /*/ is not considered to be a valid comment...
   read_ml_comment(Stream,Token).

% a string atom
read_token(C,Stream,T) :- 
   char_type(C,lower),
   !,
   read_identifier(Stream,Cs),
   atom_chars(SA,[C|Cs]),
	qualify_as_functor_or_atom(SA,Stream,T).

% a sequence of operator characters 
% (This clause has to come AFTER handling of multi-line comments!)
read_token(Op,Stream,T) :- 
   operator_char(Op),
   !,
   read_operators(Stream,OPs),
   atom_chars(AOPs,[Op|OPs]), 
	qualify_as_functor_or_atom(AOPs,Stream,T).
   
read_token(C,_Stream,_) :- char_type(C,end_of_file),!,fail.

read_token(C,Stream,_) :- 
	lexer_error(Stream,['unrecognized symbol (',C,') [FATAL]']),!,fail.



qualify_as_functor_or_atom(A,Stream,f(A)) :- peek_char(Stream,'('),!.
qualify_as_functor_or_atom(A,_Stream,a(A)).



% an identifier is a sequence of upper or lower case letters, digits and "_".
read_identifier(Stream,[C|Cs]) :-
   peek_char(Stream,C),
   (  % if...
      char_type(C,alnum) 
   ;  % or...
      C = '_' 
   ),
   !, % then ...
   get_char(Stream,_),
   read_identifier(Stream,Cs).   
read_identifier(_Stream,[]).  % this also handles the "end of file" case



read_string_with_quotations(Stream,Delimiter,[]) :- 
   at_end_of_stream(Stream,['missing delimiter: ',Delimiter]),
   !.
read_string_with_quotations(Stream,Delimiter,R) :-
   get_char(Stream,C),!,
   (  % C is the delimiter...
      C = Delimiter,!,
      R = []
   ;  % C is the start of an escape sequence...
      C = '\\',!,
      get_char(Stream,NC),
      (
         NC = '\\',!,
         R = ['\\'|Cs]
      ;
         NC = 't',!,
         R = ['\t'|Cs]
      ;
         NC = 'n',!,
         R = ['\n'|Cs]
      ;
         NC = '\'',!,
         R = ['\''|Cs]
      ;  % also handles the case that we are at the end of the stream
         lexer_error(Stream,['unsupported escape sequence (\\',NC,')']),
         R = Cs
      ),
      read_string_with_quotations(Stream,Delimiter,Cs)
   ;  % C is an "ordinary" character...
      R = [C|Cs],
      read_string_with_quotations(Stream,Delimiter,Cs)
   ).



% a squence of "operator" signs
read_operators(Stream,[C|Cs]) :-
   peek_char(Stream,C),
   operator_char(C),
   !,
   get_char(Stream,_),
   read_operators(Stream,Cs). 
read_operators(_Stream,[]). % also handles the end of file case



% an EOL comment can contain arbitrary chars and extends until the end of the line
read_eol_comment(Stream,Cs) :-
   get_char(Stream,C),
   (  ( C = '\n' ; char_type(C,end_of_file) ),!,
      Cs = []
   ;
      Cs = [C|RCs],
      read_eol_comment(Stream,RCs)
   ).



% an integer or floating point value
read_number(Stream,S1) :-
   read_int_part(Stream,S1,S2),
   read_fp_part(Stream,S2,[]).
   
   
read_fp_part(Stream,S1,SZ) :-
   peek_char(Stream,C),
   read_fp_part1(C,Stream,S1,SZ).
   
read_fp_part1(C,_Stream,[],[]) :- char_type(C,space),!.  
read_fp_part1('.',Stream,S1,SZ) :-
   !,
   stream_property(Stream,position(BeforeDot)),
   get_char(Stream,_),
   read_int_part(Stream,S2,S3),
   (                                         % if ...
      S2 = [],                               % no floating point segment
      !,                                     % is found
      set_stream_position(Stream,BeforeDot),  % then the "." is an operator
      S1 = SZ, SZ = []
   ;  % succeeded reading floating point segment
      S1 = ['.'|S2],
      (
         at_end_of_stream(Stream),
         !,
         S3 = [],
         SZ = []
      ;
         peek_char(Stream,C),
         read_fp_part2(C,Stream,S3,SZ)
      )
   ).
read_fp_part1(C,Stream,S1,SZ) :-  read_fp_part2(C,Stream,S1,SZ).

read_fp_part2(C,_Stream,[],[]) :- char_type(C,space),!.
read_fp_part2('e',Stream,S1,SZ) :- 
   !,
   get_char(Stream,_),
   S1 = ['e'|S2],
   read_exponent(Stream,S2,SZ).
read_fp_part2(C,Stream,[],[]) :-
   char_type(C,alpha),
   lexer_error(Stream,['unexpected symbol (',C,')']).
read_fp_part2(_C,_Stream,SZ,SZ). % also handles the "end of file" case

read_exponent(Stream,[],[]) :- at_end_of_stream(Stream,['exponent expected']),!.
read_exponent(Stream,S1,SZ) :-
   peek_char(Stream,C),
   (
      ( C = '-' ; C = '+' ),
      !,
      get_char(Stream,_),
      S1 = [C|SX]
   ;
      S1 = SX
   ),
   read_int_part(Stream,SX,SZ).
   


% reads the characters of an integer value using a difference list
read_int_part(Stream,[C|SY],SZ) :-
   peek_char(Stream,C),
   char_type(C,digit),
   !,
   get_char(Stream,_),
   read_int_part(Stream,SY,SZ).
read_int_part(_Stream,SZ,SZ). % also handles the "end of file" case

   
   

% a multi-line comment extends until "*/" is found
read_ml_comment(Stream,_) :- at_end_of_stream(Stream,['expected */']),!.
read_ml_comment(Stream,Token) :-
   peek_char(Stream,C),
   (
      C = '*',!,
      get_char(Stream,_),
      read_structured_ml_comment(Stream,SC_Tokens),
      Token=sc(SC_Tokens)
   ;
      read_unstructured_ml_comment(Stream,Cs),
      atom_chars(ACs,Cs),
      Token=mlc(ACs)
   ).
   

read_unstructured_ml_comment(Stream,[]) :- 
   at_end_of_stream(Stream,['expected "*/"']),
   !.
read_unstructured_ml_comment(Stream,R) :-
   get_char(Stream,C),
   (
      C = '*',peek_char(Stream,'/'),!,
      get_char(Stream,_),
      R = []
   ;
      R = [C|Cs],
      read_unstructured_ml_comment(Stream,Cs)
   ).



/*                                                                            *\
 *                           Structured Comments                              *
\*                                                                            */



read_structured_ml_comment(Stream,Tokens) :-
   stream_position(Stream,LN,CN),
 	current_stream(File,read,Stream),
   get_char(Stream,C),
   (	
      C = '*', peek_char(Stream,'/'),!,
      get_char(Stream,_),
      Tokens = []
   ;
      sc_read_token(C,Stream,T),!,
		(
			T = none,!,
			Tokens = RTs
		;
      	sc_token_with_position(T,File,LN,CN,TwithPos),
      	Tokens = [TwithPos|RTs]
		),
      read_structured_ml_comment(Stream,RTs)
   ).
read_structured_ml_comment(Stream,[]) :-  
   lexer_error(Stream,['unexpected end of file while lexing structured comment']).


sc_token_with_position(C,File,LN,CN,T) :- sc_special_char(C),!,T =.. [C,pos(File,LN,CN)].
sc_token_with_position(ws(WS),File,LN,CN,ws(WS,pos(File,LN,CN))) :- !.
sc_token_with_position(tf(TF),File,LN,CN,tf(TF,pos(File,LN,CN))) :- !.


sc_read_token(C,_Stream,C) :- sc_special_char(C),!.
sc_read_token(WS,_Stream,none) :- char_type(WS,space),!.
sc_read_token(EOF,_Stream,_) :- char_type(EOF,end_of_file),!,fail.

% the base case... a sequence of characters; i.e., a text fragment (tf)
sc_read_token(C,Stream,Token) :- 
   sc_read_tf(Stream,Cs),
   atom_chars(ATF,[C|Cs]),
   Token=tf(ATF).


sc_read_tf(Stream,Cs) :- 
   peek_char(Stream,C),
   \+ sc_tf_delimiter(C),
   !,
   get_char(Stream,_),
   Cs = [C|RCs],
   sc_read_tf(Stream,RCs).
sc_read_tf(_Stream,[]). 

sc_tf_delimiter(C) :- sc_special_char(C),!.
sc_tf_delimiter(WS) :- char_type(WS,space),!.
sc_tf_delimiter(EOF) :- char_type(EOF,end_of_file),!.



/**
	The list of all chars that have special semantics in the context of structured
	comments.
*/
sc_special_char('*') :- !.  
sc_special_char('<') :- !.
sc_special_char('{') :- !.
sc_special_char('}') :- !.
sc_special_char('(') :- !.
sc_special_char(')') :- !.
sc_special_char(',') :- !.
sc_special_char('/') :- !.
sc_special_char('>') :- !.
sc_special_char('@') :- !.



stream_position(Stream,LN,CN) :-
   line_count(Stream,LN),
   line_position(Stream,CN).



at_end_of_stream(Stream,MessageFragments) :-
   at_end_of_stream(Stream),
   atomic_list_concat(MessageFragments,EM),
   lexer_error(Stream,['unexpected end of file (',EM,')']).



lexer_error(Stream,MessageFragments) :-
   stream_position(Stream,LN,CN),
   current_stream(File,read,Stream),
   atomic_list_concat(MessageFragments,EM),
   atomic_list_concat(['ERROR:',File,':',LN,':',CN,': ',EM,'\n'],MSG),
   write(MSG).