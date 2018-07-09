/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.yarn.work.i18n;

import java.util.HashMap;
import java.util.Locale;
import java.util.ResourceBundle;
import java.text.MessageFormat;
import java.util.Objects;

public class LocalStringsManager {

    private static final String PROPS_NAME = ".LocalStrings";
    private static final HashMap<LocalStringsKey, LocalStringsManager> localStringManagers = new HashMap<LocalStringsKey, LocalStringsManager>();

    private ResourceBundle bundle;
    private Locale locale;

    private LocalStringsManager(String packageName, Locale locale) {
        String bundleName = packageName + PROPS_NAME;
        try {
            bundle = ResourceBundle.getBundle(bundleName, locale);
        } catch (Exception ex) {
            ClassLoader cl = Thread.currentThread().getContextClassLoader();
            if (cl != null) {
                try {
                    bundle = ResourceBundle.getBundle(bundleName, locale, cl);
                } catch (Exception ignore) {
                    // Ignore
                }
            }
        }

        this.locale = locale;
        if (bundle != null) {
            Locale bundleLocale = bundle.getLocale();
            if (!Objects.equals(locale, bundleLocale)) {
                this.locale = bundleLocale;
            }
        } else {
            this.locale = null;
        }
    }

    /**
     * Get a String from the caller's package's LocalStrings.properties
     *
     * @param indexString The string index into the localized string file
     * @return the String from LocalStrings or the supplied String if it doesn't
     * exist
     */
    public String get(String indexString) {
        try {
            return getBundle().getString(indexString);
        } catch (Exception e) {
            // it is not an error to have no key...
            return indexString;
        }
    }

    /**
     * Get and format a String from the caller's package's
     * LocalStrings.properties
     *
     * @param indexString The string index into the localized string file
     * @param objects The arguments to give to MessageFormat
     * @return the String from LocalStrings or the supplied String if it doesn't
     * exist -- using the array of supplied Object arguments
     */
    public String get(String indexString, Object... objects) {
        indexString = get(indexString);

        try {
            MessageFormat mf = new MessageFormat(indexString);
            mf.setLocale(locale);
            return mf.format(objects);
        } catch (Exception e) {
            return indexString;
        }
    }

    /**
     * Get a String from the caller's package's LocalStrings.properties
     *
     * @param indexString The string index into the localized string file
     * @param defaultValue
     * @return the String from LocalStrings or the supplied default value if it
     * doesn't exist
     */
    public String getString(String indexString, String defaultValue) {
        try {
            return getBundle().getString(indexString);
        } catch (Exception e) {
            // it is not an error to have no key...
            return defaultValue;
        }
    }

    /**
     * Get an integer from the caller's package's LocalStrings.properties
     *
     * @param indexString The string index into the localized string file
     * @param defaultValue
     * @return the integer value from LocalStrings or the supplied default if it
     * doesn't exist or is bad.
     */
    public int getInt(String indexString, int defaultValue) {
        try {
            String s = getBundle().getString(indexString);
            return Integer.parseInt(s);
        } catch (Exception e) {
            // it is not an error to have no key...
            return defaultValue;
        }
    }

    /**
     * Get a boolean from the caller's package's LocalStrings.properties
     *
     * @param indexString The string index into the localized string file
     * @param defaultValue
     * @return the integer value from LocalStrings or the supplied default if it
     * doesn't exist or is bad.
     */
    public boolean getBoolean(String indexString, boolean defaultValue) {
        try {
            return Boolean.valueOf(getBundle().getString(indexString));
        } catch (Exception e) {
            // it is not an error to have no key...
            return defaultValue;
        }
    }

    private ResourceBundle getBundle() {
        return bundle;
    }

    /**
     * Identify the Locale this StringManager is associated with
     *
     * @return The Locale associated with this instance
     */
    public Locale getLocale() {
        return locale;
    }

    /**
     * The LocalStringsManager will be returned for the package in which the
     * class is located. If a manager for that package already exists, it will
     * be reused, else a new StringManager will be created and returned.
     *
     * @param clazz The class for which to retrieve the StringManager
     *
     * @return The LocalStringsManager for the given class.
     */
    public static final LocalStringsManager getManager(Class<?> clazz) {
        return getManager(clazz.getPackage().getName());
    }

    /**
     * If a manager for a package already exists, it will be reused, else a new
     * LocalStringsManager will be created and returned.
     *
     * @param packageName The package name
     *
     * @return The LocalStringsManager for the given package.
     */
    public static final LocalStringsManager getManager(String packageName) {
        return getManager(packageName, Locale.getDefault());
    }

    /**
     * If a manager for a package/Locale combination already exists, it will be
     * reused, else a new LocalStringsManager will be created and returned.
     *
     * @param packageName The package name
     * @param locale The Locale
     *
     * @return The StringManager for a particular package and Locale
     */
    public static final synchronized LocalStringsManager getManager(
            String packageName, Locale locale) {

        LocalStringsKey localStringsKey = new LocalStringsKey(packageName, locale);
        LocalStringsManager mgr = localStringManagers.get(localStringsKey);
        if (mgr == null) {
            mgr = new LocalStringsManager(packageName, locale);
            localStringManagers.put(localStringsKey, mgr);
        }

        return mgr;
    }

}
