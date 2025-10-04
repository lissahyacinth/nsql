grammar NSQL;

// Parser rules
query: selectStatement EOF;

selectStatement
    : SELECT selectList FROM tableExpression whereClause? groupByClause? limitClause?
    ;

selectList
    : selectItem (',' selectItem)*
    ;

selectItem
    : expression (AS? IDENTIFIER)?
    | '*'
    ;

tableExpression
    : IDENTIFIER (AS? IDENTIFIER)? joinClause*
    ;

joinClause
    : INNER? JOIN tableExpression joinWindow ON expression
    ;

joinWindow
    : WITHIN NUMBER timeUnit
    ;

timeUnit
    : HOUR | HOURS | MINUTE | MINUTES | SECOND | SECONDS | DAY | DAYS
    ;

whereClause
    : WHERE expression
    ;

groupByClause
    : GROUP BY expression (',' expression)*
    ;

limitClause
    : LIMIT NUMBER
    ;

qualifiedIdentifier: IDENTIFIER ('.' IDENTIFIER)*;

expression
    : expression AND expression                          # andExpression
    | expression OR expression                           # orExpression
    | NOT expression                                     # notExpression
    | expression comparisonOp expression                 # comparisonExpression
    | expression LIKE STRING                             # likeExpression
    | expression IN '(' expressionList ')'               # inExpression
    | qualifiedIdentifier                                # qualifiedIdentifierExpression
    | IDENTIFIER                                         # identifierExpression
    | STRING                                             # stringExpression
    | NUMBER                                             # numberExpression
    | '(' expression ')'                                 # parenthesizedExpression
    ;

expressionList
    : expression (',' expression)*
    ;

comparisonOp
    : '=' | '!=' | '<' | '>' | '<=' | '>='
    ;

// Lexer rules
SELECT: 'SELECT';
FROM: 'FROM';
WHERE: 'WHERE';
GROUP: 'GROUP';
BY: 'BY';
LIMIT: 'LIMIT';
AS: 'AS';
INNER: 'INNER';
JOIN: 'JOIN';
WITHIN: 'WITHIN';
ON: 'ON';
AND: 'AND';
OR: 'OR';
NOT: 'NOT';
LIKE: 'LIKE';
IN: 'IN';
IS: 'IS';
TRUE: 'TRUE';
FALSE: 'FALSE';
NULL: 'NULL';

// Time units
HOUR: 'HOUR';
HOURS: 'HOURS';
MINUTE: 'MINUTE';
MINUTES: 'MINUTES';
SECOND: 'SECOND';
SECONDS: 'SECONDS';
DAY: 'DAY';
DAYS: 'DAYS';

IDENTIFIER: [a-zA-Z_][a-zA-Z0-9_]*;
STRING: '\'' (~'\'' | '\\\'')* '\'';
NUMBER: [0-9]+ ('.' [0-9]+)?;

WS: [ \t\r\n]+ -> skip;