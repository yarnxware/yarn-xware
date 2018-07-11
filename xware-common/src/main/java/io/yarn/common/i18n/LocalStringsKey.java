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
package io.yarn.common.i18n;

import java.util.Locale;
import java.util.Objects;

public class LocalStringsKey {

    private final String packageName;

    private final Locale locale;

    public LocalStringsKey(String packageName, Locale locale) {
        this.packageName = packageName;
        this.locale = locale;
    }

    public String getPackageName() {
        return packageName;
    }

    public Locale getLocale() {
        return locale;
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 97 * hash + Objects.hashCode(this.packageName);
        hash = 97 * hash + Objects.hashCode(this.locale);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final LocalStringsKey other = (LocalStringsKey) obj;
        if (!Objects.equals(this.packageName, other.packageName)) {
            return false;
        }
        if (!Objects.equals(this.locale, other.locale)) {
            return false;
        }
        return true;
    }

}
