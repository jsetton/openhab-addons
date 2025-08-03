/*
 * Copyright (c) 2010-2025 Contributors to the openHAB project
 *
 * See the NOTICE file(s) distributed with this work for additional
 * information.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0
 *
 * SPDX-License-Identifier: EPL-2.0
 */
package org.openhab.binding.insteon.internal.device.database;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.eclipse.jdt.annotation.NonNullByDefault;
import org.openhab.binding.insteon.internal.device.InsteonAddress;

/**
 * The {@link LinkDBEntry} holds a link database entry for a linked device
 *
 * @author Jeremy Setton - Initial contribution
 */
@NonNullByDefault
public class LinkDBEntry {
    private final InsteonAddress address;
    private final Set<Integer> controllers = new TreeSet<>();
    private final Set<Integer> responders = new TreeSet<>();

    public LinkDBEntry(InsteonAddress address) {
        this.address = address;
    }

    public InsteonAddress getAddress() {
        return address;
    }

    public synchronized List<Integer> getControllerGroups() {
        return controllers.stream().toList();
    }

    public synchronized List<Integer> getResponderGroups() {
        return responders.stream().toList();
    }

    public synchronized boolean hasControllerOrResponderGroups() {
        return !controllers.isEmpty() || !responders.isEmpty();
    }

    public synchronized void addControllerGroup(int group) {
        controllers.add(group);
    }

    public synchronized void addResponderGroup(int group) {
        responders.add(group);
    }

    public synchronized void removeControllerGroup(int group) {
        controllers.remove(group);
    }

    public synchronized void removeResponderGroup(int group) {
        responders.remove(group);
    }

    @Override
    public String toString() {
        String s = address + ":";
        if (controllers.isEmpty()) {
            s += " the device controls no groups";
        } else {
            s += " the device controls groups " + controllers;
        }
        if (responders.isEmpty()) {
            s += " and responds to no groups";
        } else {
            s += " and responds to groups " + responders;
        }
        return s;
    }
}
