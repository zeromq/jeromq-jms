package org.zeromq.jms;

/*
 * Copyright (c) 2015 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import java.text.ParseException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The ZERO MQ URI is loosely based on the Apache camel UTI concept.
 *
 *     jms:[queue:|topic:]destinationName[?options]
 * where
 *     "desinationName" is a JMS queue or topic name
 *     "options"        is the query options as name/value pairs in the i.e. ...?option=value&option=value&...
 *
 * Each option is prefixed with the type of attribute, i.e. zmq, jms, etc...
 *
 * It is possible to add custom attributes, to initialise custom classes.
 *
 * ...&redelivery.retry=3
 */
public class ZmqURI implements Externalizable {

    /**
     *  address:   (tcp://(\\*|((\\w+|\\.)+)):\\d+)  ==> tcp://*:123, tcp://zmq.org, etc...
     *  address:   (inproc://\\w+)                   ==> inproc://test
     *  parameter: (\\w|\\.)+                        ==> gateway, gateway.retry, etc...
     *  others:    :|\\?|=|\\&                       ==> separate on ?, =, or & symbols
     */
    private static final String TOKENISE_REGX = "(tcp://(\\*|((\\w+|\\.)+)):\\d+)|(inproc://\\w+)|(\\w|\\.)+|:|\\?|=|,|\\&";

    /* String used to construct the URI */
    private String str;
    /* URI scheme, i.e. JMS */
    private String scheme;
    /* Type of destination, i.e. queue, topic, etc... */
    private String destinationType;
    /* Name of destination */
    private String destinationName;
    /* Parameter name/value(s) of the URI */
    private Map<String, List<String>> options;

    /**
     * The token parsed from the expression.
     */
    private static final class Token {
        /* Token value */
        private final String value;
        /* Position found */
        private final int pos;

        /**
         * Token constructor.
         * @param value  the token value
         * @param pos    the position the token was found
         */
        private Token(final String value, final int pos) {
            this.value = value;
            this.pos = pos;
        }

        /**
         * Return "true" when the token contains the specified value.
         * @param value  the value to check
         * @return       return true when found
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
     * Create ONLY for the externalizable interface.
     */
    public ZmqURI() {
    }

    /**
     * Private constructor of the URI.
     * @param str              the URI as a string
     * @param scheme           the scheme
     * @param destinationType  the destination type
     * @param destinationName  the destination name
     * @param options          the parameter options
     */
    protected ZmqURI(final String str, final String scheme, final String destinationType, final String destinationName,
            final Map<String, List<String>> options) {

        this.str = str;
        this.scheme = scheme;
        this.destinationType = destinationType;
        this.destinationName = destinationName;
        this.options = options;
    }

    /**
     * Creates a URI by parsing the given string.
     * @param str                        the URI as a string to be parse
     * @throws IllegalArgumentException  throws illegal argument exception when the URI violates the definition.
     * @return                           return a URI instance for the given string
     */
    public static ZmqURI create(final String str) throws IllegalArgumentException {
        try {
            ZmqURI uri = parse(str);

            return uri;
        } catch (ParseException ex) {
            throw new IllegalArgumentException("URI string cannot be parse: " + str, ex);
        }
    }

    /**
     * Parse and return the value of the specified token. The value is validated against an expected REGEX pattern.
     * tHe function will throw a parse exception on failure.
     * @param regex            the regular expression to validate against
     * @param index            the index into the token list to obtain the token to check
     * @param tokens           the list of tokens
     * @return                 return the value value of the token
     * @throws ParseException  throws parse exception of failure
     */
    private static String parseValue(final String regex, final int index, final List<Token> tokens) throws ParseException {

        int pos = 0;
        if (0 <= (index - 1)) {
            Token prevToken = tokens.get(index - 1);
            pos = prevToken.pos + prevToken.value.length();
        }

        if (index >= tokens.size()) {
            throw new ParseException("Missing expecting value:" + regex, pos);
        }

        final String value = tokens.get(index).value;
        final Pattern pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);

        if (!pattern.matcher(value).matches()) {
            throw new ParseException("Invalid value [" + value + "] at position " + pos + " for pattern [" + regex + "]", pos);
        }

        return value;
    }

    /**
     * Parse the URI string into the expected components.
     * @param str              the string
     * @return                 return the constructed URI
     * @throws ParseException  throws parse exception of failure
     */
    private static ZmqURI parse(final String str) throws ParseException {
        final Pattern pattern = Pattern.compile(TOKENISE_REGX, Pattern.CASE_INSENSITIVE);
        final Matcher matcher = pattern.matcher(str);

        final List<Token> tokens = new ArrayList<Token>();

        int start = 0;
        while (matcher.find(start)) {
            final String token = matcher.group();
            tokens.add(new Token(token, start));
            start = matcher.end();
        }

        int index = 0;

        final String scheme = parseValue("jms", index++, tokens);
        parseValue(":", index++, tokens);
        final String destinationType = parseValue("(queue|topic)", index++, tokens);
        parseValue(":", index++, tokens);
        final String destinationName = parseValue("\\w+", index++, tokens);
        final Map<String, List<String>> options = new HashMap<String, List<String>>();

        if (tokens.size() > index) {
            parseValue("\\?", index++, tokens);

            // parse attributes
            for (int i = index; i < tokens.size(); i++) {
                final String optionName = parseValue("(\\w|\\.)+", i, tokens);

                if (optionName == null || optionName.length() == 0 || (optionName.startsWith("socket") && optionName.indexOf('.') <= 0)) {
                    final Token token = tokens.get(i);
                    throw new ParseException("Invalid tokattribute name [" + optionName + "] within token: " + token, token.pos);
                }
                // final int pos = tokens.get(i).pos;
                // default to empty and try and fill
                String optionValue = null;
                parseValue("=", ++i, tokens);
                boolean nextOptionValue = true;

                while (nextOptionValue) {
                    if (tokens.size() > i + 1) {
                        if (!tokens.get(i + 1).hasValue("\\&")) {
                            optionValue = parseValue(".*", ++i, tokens);
                       }
                    }

                    if (!options.containsKey(optionName)) {
                        // first value of class of the custom name
                        options.put(optionName, new ArrayList<String>());
                    }

                    final List<String> optionValues = options.get(optionName);
                    optionValues.add(optionValue);

                    nextOptionValue = (tokens.size() > i + 1) && tokens.get(i + 1).hasValue(",");

                    if (nextOptionValue) {
                        i++;
                    }
                }

                if (tokens.size() > i + 1) {
                    parseValue("\\&", ++i, tokens);
                }
            }
        }

        ZmqURI uri =
            new ZmqURI(str, scheme, destinationType, destinationName,
                Collections.unmodifiableMap(options));

        return uri;
    }

    /**
     * @return  return the actual string used to construct URI
     */
    protected String getStr() {
        return str;
    }

    /**
     * @return  return the schema of the Zero MQ URI
     */
    public String getScheme() {
        return scheme;
    }

    /**
     * @return  return the destination type of the Zero MQ URI
     */
    public String getDestinationType() {
        return destinationType;
    }

    /**
     * @return  return the destination name of the Zero MQ URI
     */
    public String getDestinationName() {
        return destinationName;
    }

    /**
     * Return true if the URI contains the specified option.
     * @param paramName      the parameter name
     * @return               return true when the option has been found
     */
    public boolean isOption(final String paramName) {
        return options.containsKey(paramName);
    }

    /**
     * Return the options found on the URI as a map of names to list of values.
     * @return  return the options as a name/values map
     */
    public Map<String, List<String>> getOptions() {
        return options;
    }

    /**
     * Return the options found on the URI starting with the specified prefix
     * as a map of names to list of values.
     * @param  prefix  the prefix string, i.e. "socket."
     * @return         return the options as a name/values map
     */
    public Map<String, List<String>> getOptions(final String prefix) {
        Map<String, List<String>> selectedOptions = new HashMap<String, List<String>>();

        for (String name : options.keySet()) {
            if (name.startsWith(prefix)) {
                final List<String> value = options.get(name);
                selectedOptions.put(name, value);
            }
        }
        return selectedOptions;
    }

    /**
     * Return the option values for the specified parameter name within the URI.
     * A null value is return when non can be found.
     * @param paramName      the parameter name
     * @return               return the values, or null when none
     */
    public String[] getOptionValues(final String paramName) {
        final List<String> values = options.get(paramName);

        if (values == null) {
            return null;
        }

        return values.toArray(new String[0]);
    }

    /**
     * Return the option values found for the specified parameter within the
     * URI. When non can be found the default values are returned.
     * @param paramName      the parameter name
     * @param defaultValues  the default values to return when nothing is found
     * @return               return the values, or the default values
     */
    public String[] getOptionValues(final String paramName, final String[] defaultValues) {
        String[] values = getOptionValues(paramName);

        if (values == null) {
            return defaultValues;
        }

        return values;
    }

    /**
     * Return the first option value found for the specified parameter within
     * the URI. A null value is return when non can be found.
     * @param  paramName    the parameter name
     * @return              return the first value found, or null when none
     */
    public String getOptionValue(final String paramName) {
        String[] values = getOptionValues(paramName);

        if (values == null || values.length == 0) {
            return null;
        }

        return values[0];
    }

    /**
     * Converts a possible list of values for an option to a sing;e value with a specified
     * separator.
     * @param paramName  the parameter name
     * @param seperator  the separator
     * @return           return a single value (or values in a list)
     */
    public String getOptionValue(final String paramName, final char separator) {
        String[] values = getOptionValues(paramName);

        if (values == null || values.length == 0) {
            return null;
        }

        StringBuilder builder = new StringBuilder();
        
        for (String value : values) {
            if (builder.length() > 0) {
                builder.append(separator);
            }
            builder.append(value);
        }
        
        return builder.toString();
    }

    /**
     * Return the first option value found for the specified parameter within
     * the URI. When non can be found the default value are returned.
     * @param paramName     the parameter name
     * @param defaultValue  the default value returned when nothing is found
     * @return              return the first value found, or the default value
     */
    public String getOptionValue(final String paramName, final String defaultValue) {
        String[] values = getOptionValues(paramName);

        if (values == null || values.length == 0) {
            return defaultValue;
        }

        return values[0];
    }

    /**
     * Return the first option value found for the specified BOOLEAN parameter
     * within the URI. When non can be found the default value are returned.
     * @param  paramName     the parameter name
     * @param  defaultValue  the default value returned when nothing is found
     * @return return        return the first value found, or the default value
     */
    public boolean getOptionValue(final String paramName, final boolean defaultValue) {
        String[] values = getOptionValues(paramName);

        if (values == null || values.length == 0) {
            return defaultValue;
        }

        return Boolean.parseBoolean(values[0]);
    }

    @Override
    public int hashCode() {
        final int prime = 31;

        int result = 1;

        result = prime * result + ((destinationName == null) ? 0 : destinationName.hashCode());
        result = prime * result + ((str == null) ? 0 : str.hashCode());

        return result;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null) {
            return false;
        }

        if (getClass() != obj.getClass()) {
            return false;
        }

        ZmqURI other = (ZmqURI) obj;

        if (destinationName == null) {
            if (other.destinationName != null) {
                return false;
            }
        } else if (!destinationName.equals(other.destinationName)) {
            return false;
        }

        if (str == null) {
            if (other.str != null) {
                return false;
            }
        } else if (!str.equals(other.str)) {
            return false;
        }

        return true;
    }

    @Override
    public String toString() {
        return "ZmqURI [str=" + str + "]";
    }

    @Override
    public void writeExternal(final ObjectOutput out) throws IOException {
        out.writeObject(str);
        out.writeObject(scheme);
        out.writeObject(destinationType);
        out.writeObject(destinationName);

        out.writeInt(options.size());

        for (String name : options.keySet()) {
            final List<String> values = options.get(name);

            out.writeInt(values.size());

            for (String value : values) {
                out.writeObject(value);
            }
        }
    }

    @Override
    public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
        str = (String) in.readObject();
        scheme = (String) in.readObject();
        destinationType = (String) in.readObject();
        destinationName = (String) in.readObject();
        options = new HashMap<String, List<String>>();

        final int optionCount = in.readInt();

        for (int i = 0; i < optionCount; i++) {
            final String optionName = (String) in.readObject();
            final int valueCount = in.readInt();

            if (!options.containsKey(optionName)) {
                // first value of class of the custom name
                options.put(optionName, new ArrayList<String>());
            }

            final List<String> optionValues = options.get(optionName);

            for (int j = 0; j < valueCount; j++) {
                final String optionValue = (String) in.readObject();

                optionValues.add(optionValue);
            }
        }
    }
}
