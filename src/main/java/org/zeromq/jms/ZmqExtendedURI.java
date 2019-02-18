package org.zeromq.jms;
/*
 * Copyright (c) 2016 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
/**
 * Implements "extends" for URI inheritance.
 */
public class ZmqExtendedURI extends ZmqURI {

	private static final String LABEL_EXTENDS = "extends"; 
    private final List<ZmqURI> extendURIs;

    /**
     * Construct the URI around ant existing URI with the URI schema.
     * @param uri                the URI with possible inheritance
     * @param destinationSchema  the schema of URIs
     */
    public ZmqExtendedURI(final ZmqURI uri, final Map<String, ZmqURI> destinationSchema) {
        super(uri.getStr(), uri.getScheme(), uri.getDestinationType(), uri.getDestinationName(), uri.getOptions());

        extendURIs = new ArrayList<ZmqURI>(getExtends(uri, destinationSchema));

        Collections.reverse(extendURIs);
    }

    /**
     * Find all the URI related to the specified URI by the "extends" attribute.
     * @param uri                the URI
     * @param destinationSchema  the schema of URIs
     * @return                   return a URI related to each other in oldest order first
     */
    private Set<ZmqURI> getExtends(final ZmqURI uri, final Map<String, ZmqURI> destinationSchema) {
        final Set<ZmqURI> newExtendURIs = new LinkedHashSet<ZmqURI>();

        if (uri.isOption(LABEL_EXTENDS)) {
            final String[] names = uri.getOptionValues(LABEL_EXTENDS);
            // ensure the last extend has the lowest priority
            Collections.reverse(Arrays.asList(names));
            for (String name : names) {
                final ZmqURI extendURI = destinationSchema.get(name);
                final Set<ZmqURI> extendURIs = getExtends(extendURI, destinationSchema);

                newExtendURIs.addAll(extendURIs);
            }
        }

        newExtendURIs.add(uri);

        return newExtendURIs;
    }

    /**
     * Return "true" when the specified URIL has extensions.
     * @param uri  the URI to check
     * @return     return true when it has an "extend"
     */
    public static boolean isExtened(final ZmqURI uri) {
        return uri.isOption(LABEL_EXTENDS);
    }

    /**
     * Return true if the URI contains the specified option.
     * @param paramName      the parameter name
     * @return               return true when the option has been found
     */
    public boolean isOption(final String paramName) {
        for (ZmqURI uri : extendURIs) {
            if (uri.isOption(paramName)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Return the options found on the URI as a map of names to list of values.
     * @return  return the options as a name/values map
     */
    public Map<String, List<String>> getOptions() {
        Map<String, List<String>> options = new HashMap<String, List<String>>();

        for (ZmqURI uri : extendURIs) {
            options.putAll(uri.getOptions());
        }

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

        for (ZmqURI uri : extendURIs) {
            Map<String, List<String>> options = uri.getOptions();

            for (String name : options.keySet()) {
                if (name.startsWith(prefix)) {
                    final List<String> value = options.get(name);
                    selectedOptions.put(name, value);
                }
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
        for (ZmqURI uri : extendURIs) {
            final Map<String, List<String>> options = uri.getOptions();
            final List<String> values = options.get(paramName);

            if (values != null) {
                return values.toArray(new String[0]);
            }
        }

        return null;
    }
}
