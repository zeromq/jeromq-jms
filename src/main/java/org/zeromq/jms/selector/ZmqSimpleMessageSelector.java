package org.zeromq.jms.selector;

/*
 * Copyright (c) 2015 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
import java.text.DecimalFormat;
import java.text.Format;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *  Selectors are defined using SQL 92 syntax and typically apply to JMS message headers.
 *
 *  This is a simple implementation of a SQL condition evaluation functionality. This process parse an expression
 *  using regular expression, and then converts the tokens into terms in reverse polish notation.
 *
 *  I cannot take the glory for all this hard work. The expression construction is based on ideas (but no source code) of
 *  the following article and source
 *
 *      http://udojava.com/2012/12/16/java-expression-parser-evaluator/
 *      https://github.com/jDTOBinder
 *
 *  I have simplified and enhanced it for SQL conditions, and it was only created to allow the possibility have having
 *  no JAR dependency.
 *
 *  This is only a basic implementation and is not very extensible, but its does not have too since it in SQL only.
 *
 *  This evaluator is thread safe and immutable.
 *
 *  In summary an instance is create when parsing a string expression. This instance is a wrapper for a tree structure on
 *  terms generated from the parsing of the string expression. A parse exception will be thrown if the expression cannot be
 *  parse successfully.
 *
 *  NOTE: ordering of tokens is based on operator presidence. THe higher the value the more important.
 *
 *  The instance can then be called along with a collection of variables to evaluate the parse condition.
 */
public class ZmqSimpleMessageSelector implements ZmqMessageSelector {

    // The REGX pattern used to tokenised the string expression
    private static final String TOKENISE_REGX = "=|!=|<=|>=|\\|\\||\\&\\&|\\d+\\.\\d+|\\d+|'(.*?)'|\"(.*?)\"|IS NULL|IS NOT NULL|,|\\w+|[()+\\-*/<>]";

    /**
     * Enumeration of valid operators.
     */
    enum Operator {

        LIST(",", 1), OR("OR", 1), IS_NULL("IS NULL", 1), IS_NOT_NULL("IS NOT NULL", 1), BETWEEN("BETWEEN", 1),

        AND("AND", 2),

        ROUND("ROUND", 7), LEN("LEN", 7), NOW("NOW", 7), FORMAT("FORMAT", 7), LCASE("LCASE", 7), UCASE("UCASE", 7), MID("MID", 7),

        NOT("NOT", 3),

        EQUAL("=", 4), GREATER(">", 4), GREATER_EQUAL(">=", 4), LESS("<", 4), LESS_EQUAL("<=", 4), NOT_EQUAL("<>", 4),

        ADDITION("+", 5), SUBSTRACT("-", 5),

        MULTIPLY("*", 6), DIVISION("//", 6),

        POW("^", 7),

        IN("IN", 8), LIKE("LIKE", 8);

        private static Map<String, Operator> operators;

        private final String symbol;
        private final int priority;

        /**
         * Construct operator.
         * @param symbol    the symbol
         * @param priority  the priority
         */
        Operator(final String symbol, final int priority) {
            this.symbol = symbol;
            this.priority = priority;

            mapOperator(symbol);
        }

        /**
         * Add operator to the map of operators.
         * @param symbol  the symbol
         */
        private synchronized void mapOperator(final String symbol) {

            if (operators == null) {
                operators = new HashMap<String, Operator>();
            }

            operators.put(symbol.toUpperCase(), this);
        }

        /**
         * @return  return the symbol
         */
        public String getSymbol() {
            return symbol;
        }

        /**
         * @return  return the priority
         */
        public int getPriority() {
            return priority;
        }

        /**
         * Return true when specified system show be done before.
         * @param symbol  the symbol
         * @return        return true when higher priority
         */
        boolean precedence(final String symbol) {
            Operator other = getOperator(symbol.toUpperCase());

            if (other == null) {
                return false;
            }

            return priority < other.priority;
        }

        /**
         * Return the specified token operation as a enumerator.
         * @param symbol  the symbol as a string token
         * @return        return the enumeration for that symbol
         */
        public static Operator getOperator(final String symbol) {
            return operators.get(symbol.toUpperCase());
        }

        @Override
        public String toString() {
            return symbol;
        }
    }

    /**
     * The token parsed from the expression.
     */
    private static class Token {
        private final String value;
        private final int pos;

        /**
         * Construct the token around a value and a position.
         * @param value  the value
         * @param pos    the position
         */
        Token(final String value, final int pos) {
            this.value = value;
            this.pos = pos;
        }

        /**
         * Return true when the token contain the specified value.
         * @param value  the token as a string
         * @return       return true when they are the same
         */
        private boolean hasValue(final String value) {
            return this.value.equals(value);
        }

        @Override
        public String toString() {
            return "Token [value=" + value + ", pos=" + pos + "]";
        }
    }

    /**
     * The term interface for call terms generated.
     */
    private interface ExpressionTerm {

    }

    /**
     * Literal term class, i.e. String ,number, date, etc...
     */
    private static class LiteralTerm implements ExpressionTerm {
        private final Object literal;

        /**
         * Construct a "literal" term.
         * @param literal  the lieteral
         */
        LiteralTerm(final Object literal) {
            this.literal = literal;
        }

        @Override
        public String toString() {
            return "LiteralTerm [literal=" + literal + "]";
        }
    }

    /**
     * Variable term that will be resolved against a list of properties during evaluation.
     */
    private static class VariableTerm implements ExpressionTerm {
        private final String name;

        /**
         * Construct a "variable" term.
         * @param name  the variable
         */
        VariableTerm(final String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return "VariableTerm [name=" + name + "]";
        }
    }

    /**
     * List of terms determined by the use of the "," operator.
     */
    private static class ListTerm implements ExpressionTerm {
        private final List<ExpressionTerm> terms = new ArrayList<ExpressionTerm>();

        /**
         * Construct list of terms what are comma separated.
         * @param term  the term
         */
        ListTerm(final ExpressionTerm term) {
            this.terms.add(term);
        }

        @Override
        public String toString() {
            return "ListTerm [terms=" + terms + "]";
        }
    }

    /**
     * Function term that a executed on evaluation, i.e. FORMAT(..), LIKE, IN, etc...
     */
    private static class FunctionTerm implements ExpressionTerm {
        private final Operator operator;
        private final ExpressionTerm[] parameters;

        /**
         * Construct a "function" based term.
         * @param operator   the operator
         * @param parameter  the parameter(s)
         */
        FunctionTerm(final Operator operator, final ExpressionTerm... parameter) {
            this.operator = operator;
            this.parameters = parameter;
        }

        @Override
        public String toString() {
            return "FunctionTerm [operator=" + operator + ", parameters=" + Arrays.toString(parameters) + "]";
        }
    }

    /**
     * Compound term for a binary operation, i.e. AND, OR, +, *, etc...
     */
    private static class CompoundTerm implements ExpressionTerm {
        private final Operator operator;
        private final ExpressionTerm leftTerm;
        private final ExpressionTerm rightTerm;

        /**
         * Construct a compound term.
         * @param operator   the operator
         * @param leftTerm   the left term
         * @param rightTerm  the right term
         */
        CompoundTerm(final Operator operator, final ExpressionTerm leftTerm, final ExpressionTerm rightTerm) {
            this.operator = operator;
            this.leftTerm = leftTerm;
            this.rightTerm = rightTerm;
        }

        @Override
        public String toString() {
            return "CompoundTerm [operator=" + operator + ", leftTerm=" + leftTerm + ", rightTerm=" + rightTerm + "]";
        }
    }

    /**
     * THe root expression were evulation stars.
     */
    private final ExpressionTerm expressionTerm;

    /**
     * Construct a message selector for the specified "expression" term.
     * @param expressionTerm  the expression
     */
    public ZmqSimpleMessageSelector(final ExpressionTerm expressionTerm) {
        this.expressionTerm = expressionTerm;
    }

    /**
     * Parse the expression return and newly construct and immutable .
     * @param expression       the string expression to parse
     * @return                 return the selector ready to perform evaluations
     * @throws ParseException  throws parse exception
     */
    public static ZmqMessageSelector parse(final String expression) throws ParseException {
        final Pattern pattern = Pattern.compile(TOKENISE_REGX, Pattern.CASE_INSENSITIVE);
        final Matcher matcher = pattern.matcher(expression);

        // Tokenise the expression
        LinkedList<Token> tokens = new LinkedList<Token>();

        tokens.add(new Token("(", -1));

        int start = 0;
        while (matcher.find(start)) {
            final String token = matcher.group();
            tokens.add(new Token(token, matcher.end() - token.length()));
            start = matcher.end();
        }

        tokens.add(new Token(")", -1));

        // Process the tokens
        LinkedList<Token> postfixExpr = new LinkedList<Token>();
        LinkedList<Token> precedenceStack = new LinkedList<Token>();

        int index = 0;
        while (!tokens.isEmpty() && index < tokens.size()) {
            Token token = tokens.pop();

            // check for left parentheses
            if (token.hasValue("(")) {
                precedenceStack.push(token);
                continue;
            }

            // check if it is an operator
            Operator operator = Operator.getOperator(token.value);
            if (operator != null) {

                while (operator.precedence(precedenceStack.peek().value)) {
                    postfixExpr.add(precedenceStack.pop());
                }
                precedenceStack.push(token);
                continue;
            }

            // check for right parenthesis
            if (token.hasValue(")")) {
                while (!(precedenceStack.peek().hasValue("("))) {
                    final Token stackElement = precedenceStack.pop();

                    if (Operator.getOperator(stackElement.value) != null) {
                        postfixExpr.add(stackElement);
                    }
                }

                // remove the extra parenthesis
                precedenceStack.pop();
                continue;
            }

            // otherwise add the token into the expression
            postfixExpr.add(token);
        }

        // Convert the postfix expression into nested conditions.
        if (!precedenceStack.isEmpty()) {
            throw new IllegalArgumentException("Could not parse expression!");
        }

        ExpressionTerm expressionTerm = parsePostFixExpr(postfixExpr);
        ZmqMessageSelector selector = new ZmqSimpleMessageSelector(expressionTerm);

        return selector;
    }

    /**
     * Construct a tree of terms based on the tokens in postfix (reverse polish notation).
     * @param postfixExpr      the list/stack of tokens
     * @return                 return the root term expression
     * @throws ParseException  throws parse exception fr invalid token syntax, etc...
     */
    private static ExpressionTerm parsePostFixExpr(final List<Token> postfixExpr) throws ParseException {

        final LinkedList<ExpressionTerm> termStack = new LinkedList<ExpressionTerm>();

        for (Token token : postfixExpr) {
            Operator operator = Operator.getOperator(token.value);

            if (operator == null) {
                ExpressionTerm term = buildTerm(token);
                termStack.push(term);
            } else if (operator == Operator.LIST) {
                ExpressionTerm term1 = termStack.pop();
                ExpressionTerm term2 = termStack.pop();

                ListTerm listTerm = (term1 instanceof ListTerm) ? (ListTerm) term1 : new ListTerm(term1);
                listTerm.terms.add(term2);
                termStack.push(listTerm);
            } else if (operator == Operator.BETWEEN) {
                ExpressionTerm andTerm = termStack.pop();

                if (!(andTerm instanceof CompoundTerm) || ((CompoundTerm) andTerm).operator != Operator.AND) {
                    throw new ParseException("Missing 'and' for BETWEEN clause.", token.pos);
                }

                ExpressionTerm term = termStack.pop();

                final ExpressionTerm fromTerm = ((CompoundTerm) andTerm).leftTerm;
                final ExpressionTerm toTerm = ((CompoundTerm) andTerm).rightTerm;

                ExpressionTerm betweenTerm = new FunctionTerm(operator, term, fromTerm, toTerm);
                termStack.push(betweenTerm);
            } else if (operator == Operator.LIKE) {
                ExpressionTerm patternTerm = termStack.pop();
                ExpressionTerm leftTerm = termStack.pop();

                ExpressionTerm term = new FunctionTerm(operator, leftTerm, patternTerm);
                termStack.push(term);
            } else if (operator == Operator.IS_NULL || operator == Operator.IS_NOT_NULL) {
                ExpressionTerm leftTerm = termStack.pop();

                ExpressionTerm term = new FunctionTerm(operator, leftTerm);
                termStack.push(term);
            } else if (operator == Operator.IN) {

                ExpressionTerm listTerms = termStack.pop();
                ExpressionTerm inTerm = termStack.pop();

                ExpressionTerm term = new FunctionTerm(operator, inTerm, listTerms);
                termStack.push(term);
            } else if ((operator == Operator.ROUND) || (operator == Operator.LEN) || (operator == Operator.FORMAT) || (operator == Operator.LCASE)
                    || (operator == Operator.UCASE) || (operator == Operator.MID)) {

                ExpressionTerm listTerms = termStack.pop();

                ExpressionTerm term = new FunctionTerm(operator, listTerms);
                termStack.push(term);
            } else if (operator == Operator.NOW) {
                ExpressionTerm term = new FunctionTerm(operator);
                termStack.push(term);
            } else {
                ExpressionTerm rightTerm = termStack.pop();
                ExpressionTerm leftTerm = termStack.pop();

                ExpressionTerm term = new CompoundTerm(operator, leftTerm, rightTerm);
                termStack.push(term);
            }
        }

        return termStack.pop();
    }

    /**
     * Return a unary term based on the specified token. Check are made on the token to determine a number, date, string etc...
     * @param token  the token
     * @return       the resulting term
     */
    private static ExpressionTerm buildTerm(final Token token) {
        if (Pattern.matches("[-+]?\\d*\\.\\d+", token.value)) {
            return new LiteralTerm(Double.parseDouble(token.value));
        }

        if (Pattern.matches("[-+]?\\d+", token.value)) {
            return new LiteralTerm(Integer.parseInt(token.value));
        }

        if (Pattern.matches("true|false", token.value)) {
            return new LiteralTerm(Boolean.parseBoolean(token.value));
        }

        if (token.value.startsWith("'")) {
            final int endOfLiteral = (token.value.endsWith("'")) ? token.value.length() - 1 : token.value.length();

            return new LiteralTerm(token.value.substring(1, endOfLiteral));
        }
        if (token.value.startsWith("\"")) {
            final int endOfLiteral = (token.value.endsWith("\"")) ? token.value.length() - 1 : token.value.length();

            return new LiteralTerm(token.value.substring(1, endOfLiteral));
        }

        return new VariableTerm(token.value);
    }

    @Override
    /**
     * Evaluate the parsed expression starting from the root of the tree of terms. The function will return the
     * result generated from the expression. This can be True/False, a Number, a String, anything.
     * @param variables  the map of variable values
     * @return           return the result.
     */
    public boolean evaluate(final Map<String, Object> variables) {
        Object result = evaluate(variables, expressionTerm);

        if (result instanceof Boolean) {
            return (Boolean) result;
        }

        throw new ArithmeticException("Expression does not evaulate to a boolean.");
    }

    /**
     * Evaluation of a binary operator. TTHe left and right terms have been resolvers to phyisical values.
     * @param leftValue   the left value
     * @param operator    the operator, i.e. ADDITION, AND, OR, etc....
     * @param rightValue  the right value
     * @return            return the resulting value
     */
    private Object evaluate(final Object leftValue, final Operator operator, final Object rightValue) {
        switch (operator) {
        case EQUAL:
        case GREATER:
        case GREATER_EQUAL:
        case LESS:
        case LESS_EQUAL:
        case NOT_EQUAL:
            if (leftValue instanceof Number || rightValue instanceof Number) {
                final double value1 = ((Number) leftValue).doubleValue();
                final double value2 = ((Number) rightValue).doubleValue();

                switch (operator) {
                case EQUAL:
                    return (value1 == value2);
                case GREATER:
                    return (value1 > value2);
                case GREATER_EQUAL:
                    return (value1 >= value2);
                case LESS:
                    return (value1 < value2);
                case LESS_EQUAL:
                    return (value1 <= value2);
                case NOT_EQUAL:
                    return (value1 != value2);
                default:
                    throw new ArithmeticException("Unsupported compound operator: " + operator);
                }
            }
            @SuppressWarnings("unchecked")
            final Comparable<Object> value = (Comparable<Object>) leftValue;
            final int result = value.compareTo(rightValue);

            switch (operator) {
            case EQUAL:
                return (result == 0);
            case GREATER:
                return (result > 0);
            case GREATER_EQUAL:
                return (result >= 0);
            case LESS:
                return (result < 0);
            case LESS_EQUAL:
                return (result <= 0);
            case NOT_EQUAL:
                return (result != 0);
            default:
                throw new ArithmeticException("Unsupported compound operator: " + operator);
            }

        case ADDITION:
            if (leftValue instanceof String || rightValue instanceof String) {
                return leftValue.toString() + rightValue.toString();
            }

            final double addition1 = ((Number) leftValue).doubleValue();
            final double addition2 = ((Number) rightValue).doubleValue();

            return addition1 + addition2;

        case SUBSTRACT:
        case MULTIPLY:
        case DIVISION:
        case POW:
            final double value1 = ((Number) leftValue).doubleValue();
            final double value2 = ((Number) rightValue).doubleValue();

            switch (operator) {
            case SUBSTRACT:
                return value1 - value2;
            case MULTIPLY:
                return value1 * value2;
            case DIVISION:
                return value1 / value2;
            case POW:
                return Math.pow(value1, value2);
            default:
                throw new ArithmeticException("Unsupported compound operator: " + operator);
            }

        case AND:
            return ((Boolean) leftValue && (Boolean) rightValue);

        case OR:
            return ((Boolean) leftValue || (Boolean) rightValue);

        default:
            throw new ArithmeticException("Unsupported compound operator: " + operator);
        }

    }

    /**
     * Attempt to retrieve the "valid" value from the variables. All numbers are converted to double to ensure
     * simple comparators work.
     * @param variables  the map of variable values
     * @param name       the name of the variable
     * @return           return the variable value in the correct type
     */
    private Object getValue(final Map<String, Object> variables, final String name) {
        final Object value = variables.get(name);

        if (value == null || value instanceof String || value instanceof Date) {
            return value;
        }

        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }

        throw new ArithmeticException("Unsupported type (" + value.getClass() + ") for variable: " + name);
    }

    /**
     * Evaluate the specified term return the result generated from the expression. This can be True/False, a Number, a String, anything.
     * @param variables  the map of variable values
     * @param term       the term to be evaluated
     * @return           return the result.
     */
    private Object evaluate(final Map<String, Object> variables, final ExpressionTerm term) {

        if (term instanceof LiteralTerm) {
            return ((LiteralTerm) term).literal;
        } else if (term instanceof VariableTerm) {
            final String name = ((VariableTerm) term).name;
            final Object value = getValue(variables, name);

            return value;
        } else if (term instanceof ListTerm) {
            // Evaluate all terms within the list
            ListTerm listTerms = (ListTerm) term;

            Object[] results = new Object[listTerms.terms.size()];
            for (int i = 0; i < listTerms.terms.size(); i++) {
                results[i] = evaluate(variables, listTerms.terms.get(i));
            }

            return results;
        } else if (term instanceof FunctionTerm) {

            FunctionTerm function = (FunctionTerm) term;

            // resolve evaluate the parameters.
            Object[] results = null;
            if (function.parameters != null) {
                results = new Object[function.parameters.length];
                for (int i = 0; i < function.parameters.length; i++) {
                    results[i] = evaluate(variables, function.parameters[i]);
                }
            }

            switch (function.operator) {
            case BETWEEN:
                return (Boolean) evaluate(results[0], Operator.GREATER_EQUAL, results[1])
                        && (Boolean) evaluate(results[0], Operator.LESS, results[2]);
            case LIKE:
                final String pattern = ((String) results[1]).replaceAll("_", ".").replaceAll("%", ".*");
                if (results[0] == null) {
                    return false;
                } else {
                    return Pattern.matches(pattern, (String) results[0]);
                }
            case IS_NULL:
                return (results[0] == null);
            case IS_NOT_NULL:
                return (results[0] != null);
            case IN:
                if (results[1] instanceof Object[]) {
                    for (Object result : (Object[]) results[1]) {
                        if ((Boolean) evaluate(results[0], Operator.EQUAL, result)) {
                            return true;
                        }
                    }
                    return false;
                }

                return (Boolean) evaluate(results[0], Operator.EQUAL, results[1]);

            case NOW:
                return new Date();

            case ROUND:
                final double roundValue = ((Number) results[0]).doubleValue();
                return new Integer(new Long(Math.round(roundValue)).intValue());

            case LEN:
                final String lenValue = ((String) results[0]);
                return new Integer(lenValue.length());

            case FORMAT:
                final Object[] formatParams = (Object[]) results[0];

                final String formatPattern = ((String) formatParams[0]);
                final Object formatValue = formatParams[1];

                Format format;

                if (formatValue instanceof Date) {
                    format = new SimpleDateFormat(formatPattern);
                } else if (formatValue instanceof Number) {
                    format = new DecimalFormat(formatPattern);
                } else {
                    throw new ArithmeticException("Unsupported function: " + term);
                }

                return format.format(formatValue);

            case LCASE:
                final String lcaseValue = ((String) results[0]);
                return lcaseValue.toLowerCase();

            case UCASE:
                final String ucaseValue = ((String) results[0]);
                return ucaseValue.toUpperCase();

            case MID:
                final Object[] midParams = (Object[]) results[0];

                if (midParams.length < 3) {
                    final String midValue = ((String) midParams[1]);
                    final int midStart = ((Integer) midParams[0]) - 1;

                    return midValue.substring(midStart);
                }

                final String midValue = ((String) midParams[2]);
                final int midStart = ((Integer) midParams[1]) - 1;
                final int midLen = (Integer) midParams[0];

                return midValue.substring(midStart, midStart + midLen);

            default:
                throw new ArithmeticException("Unsupported function operator: " + term);
            }

        } else if (term instanceof CompoundTerm) {
            CompoundTerm function = (CompoundTerm) term;

            Object leftResult = evaluate(variables, function.leftTerm);
            Object rightResult = evaluate(variables, function.rightTerm);

            // CHECKSTYLE:OFF: Empty Block
            try {
                return evaluate(leftResult, function.operator, rightResult);
            } catch (Exception ex) {
            }
            // CHECKSTYLE:ON: Empty Block
        }

        throw new ArithmeticException("Unable to evaulate ExpressionTerm: " + term);
    }

    /**
     * Dump the expression.
     */
    public void dump() {
        System.out.println("Expression: " + expressionTerm);
    }

    @Override
    public String toString() {
        return "ZmqSimpleMessageSelector [expressionTerm=" + expressionTerm + "]";
    }

}
