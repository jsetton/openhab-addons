/**
 * Copyright (c) 2010-2024 Contributors to the openHAB project
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
package org.openhab.binding.insteon.internal.database;

import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Stream;

import org.eclipse.jdt.annotation.NonNullByDefault;
import org.eclipse.jdt.annotation.Nullable;
import org.openhab.binding.insteon.internal.device.InsteonAddress;
import org.openhab.binding.insteon.internal.device.InsteonDevice;
import org.openhab.binding.insteon.internal.device.InsteonModem;
import org.openhab.binding.insteon.internal.device.InsteonScene;
import org.openhab.binding.insteon.internal.manager.DatabaseManager;
import org.openhab.binding.insteon.internal.utils.HexUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link LinkDB} holds all-link database records for a device
 *
 * @author Jeremy Setton - Initial contribution
 */
@NonNullByDefault
public class LinkDB {
    public static final int RECORD_BYTE_SIZE = 8;

    private static enum DatabaseStatus {
        EMPTY,
        COMPLETE,
        PARTIAL,
        LOADING
    }

    public static enum ReadWriteMode {
        STANDARD,
        PEEK_POKE,
        UNKNOWN
    }

    private final Logger logger = LoggerFactory.getLogger(LinkDB.class);

    private InsteonDevice device;
    private TreeMap<Integer, LinkDBRecord> records = new TreeMap<>(Collections.reverseOrder());
    private TreeMap<Integer, LinkDBRecord> changes = new TreeMap<>(Collections.reverseOrder());
    private DatabaseStatus status = DatabaseStatus.EMPTY;
    private int delta = -1;
    private int firstLocation = 0x0FFF;
    private boolean reload = false;
    private boolean update = false;

    public LinkDB(InsteonDevice device) {
        this.device = device;
    }

    private @Nullable InsteonModem getModem() {
        return device.getModem();
    }

    public @Nullable DatabaseManager getDatabaseManager() {
        return Optional.ofNullable(getModem()).map(InsteonModem::getDBM).orElse(null);
    }

    public int getDatabaseDelta() {
        return delta;
    }

    public int getFirstRecordLocation() {
        return firstLocation;
    }

    public int getLastRecordLocation() {
        synchronized (records) {
            return records.isEmpty() ? getFirstRecordLocation() : records.lastKey();
        }
    }

    public @Nullable LinkDBRecord getFirstRecord() {
        synchronized (records) {
            return records.isEmpty() ? null : records.firstEntry().getValue();
        }
    }

    public int getFirstRecordComponentId() {
        return Optional.ofNullable(getFirstRecord()).map(LinkDBRecord::getComponentId).orElse(0);
    }

    public @Nullable LinkDBRecord getRecord(int location) {
        synchronized (records) {
            return records.get(location);
        }
    }

    public List<LinkDBRecord> getRecords() {
        synchronized (records) {
            return records.values().stream().toList();
        }
    }

    private Stream<LinkDBRecord> getRecords(@Nullable InsteonAddress address, @Nullable Integer group,
            @Nullable Boolean isController, @Nullable Boolean isActive, @Nullable Integer componentId) {
        return getRecords().stream()
                .filter(record -> (address == null || record.getAddress().equals(address))
                        && (group == null || record.getGroup() == group)
                        && (isController == null || record.isController() == isController)
                        && (isActive == null || record.isActive() == isActive)
                        && (componentId == null || record.getComponentId() == componentId));
    }

    public List<LinkDBRecord> getControllerRecords() {
        return getRecords(null, null, true, true, null).toList();
    }

    public List<LinkDBRecord> getControllerRecords(InsteonAddress address) {
        return getRecords(address, null, true, true, null).toList();
    }

    public List<LinkDBRecord> getControllerRecords(InsteonAddress address, int group) {
        return getRecords(address, group, true, true, null).toList();
    }

    public List<LinkDBRecord> getResponderRecords() {
        return getRecords(null, null, false, true, null).toList();
    }

    public List<LinkDBRecord> getResponderRecords(InsteonAddress address) {
        return getRecords(address, null, false, true, null).toList();
    }

    public List<LinkDBRecord> getResponderRecords(InsteonAddress address, int group) {
        return getRecords(address, group, false, true, null).toList();
    }

    public @Nullable LinkDBRecord getActiveRecord(InsteonAddress address, int group, boolean isController,
            int componentId) {
        return getRecords(address, group, isController, true, componentId).findFirst().orElse(null);
    }

    public boolean hasRecord(@Nullable InsteonAddress address, @Nullable Integer group, @Nullable Boolean isController,
            @Nullable Boolean isActive, @Nullable Integer componentId) {
        return getRecords(address, group, isController, isActive, componentId).findAny().isPresent();
    }

    public boolean hasComponentIdRecord(int componentId, boolean isController) {
        return getRecords(null, null, isController, true, componentId).findAny().isPresent();
    }

    public boolean hasGroupRecord(int group, boolean isController) {
        return getRecords(null, group, isController, true, null).findAny().isPresent();
    }

    public int size() {
        return getRecords().size();
    }

    public int getLastPendingChangeLocation() {
        synchronized (changes) {
            return changes.isEmpty() ? getFirstRecordLocation() : changes.lastKey();
        }
    }

    public List<LinkDBRecord> getPendingChanges() {
        synchronized (changes) {
            return changes.values().stream().toList();
        }
    }

    private Stream<LinkDBRecord> getPendingChanges(@Nullable InsteonAddress address, @Nullable Integer group,
            @Nullable Boolean isController, @Nullable Integer componentId) {
        return getPendingChanges().stream()
                .filter(record -> (address == null || record.getAddress().equals(address))
                        && (group == null || record.getGroup() == group)
                        && (isController == null || record.isController() == isController)
                        && (componentId == null || record.getComponentId() == componentId));
    }

    public @Nullable LinkDBRecord getPendingChange(InsteonAddress address, int group, boolean isController,
            int componentId) {
        return getPendingChanges(address, group, isController, componentId).findFirst().orElse(null);
    }

    public @Nullable LinkDBRecord pollPendingChange() {
        synchronized (changes) {
            return Optional.ofNullable(changes.pollFirstEntry()).map(Entry::getValue).orElse(null);
        }
    }

    public boolean isComplete() {
        return status == DatabaseStatus.COMPLETE;
    }

    public boolean shouldReload() {
        return reload;
    }

    public boolean shouldUpdate() {
        return update;
    }

    public synchronized void setDatabaseDelta(int delta) {
        if (logger.isTraceEnabled()) {
            logger.trace("setting link db delta to {} for {}", delta, device.getAddress());
        }
        this.delta = delta;
    }

    public synchronized void setFirstRecordLocation(int firstLocation) {
        if (logger.isTraceEnabled()) {
            logger.trace("setting link db first record location to {} for {}", HexUtils.getHexString(firstLocation),
                    device.getAddress());
        }
        this.firstLocation = firstLocation;
    }

    public synchronized void setReload(boolean reload) {
        if (logger.isTraceEnabled()) {
            logger.trace("setting link db reload to {} for {}", reload, device.getAddress());
        }
        this.reload = reload;
    }

    private synchronized void setUpdate(boolean update) {
        if (logger.isTraceEnabled()) {
            logger.trace("setting link db update to {} for {}", update, device.getAddress());
        }
        this.update = update;
    }

    private synchronized void setStatus(DatabaseStatus status) {
        if (logger.isTraceEnabled()) {
            logger.trace("setting link db status to {} for {}", status, device.getAddress());
        }
        this.status = status;
    }

    /**
     * Returns a pending change location for a given address, group, controller flag and component id
     *
     * @param address the change address
     * @param group the change group
     * @param isController if controller change
     * @param componentId the change componentId
     * @return record or change location if match, otherwise next available location
     */
    public int getPendingChangeLocation(InsteonAddress address, int group, boolean isController, int componentId) {
        LinkDBRecord record = getActiveRecord(address, group, isController, componentId);
        if (record != null) {
            return record.getLocation();
        }
        LinkDBRecord change = getPendingChange(address, group, isController, componentId);
        if (change != null) {
            return change.getLocation();
        }
        return getNextAvailableLocation();
    }

    /**
     * Returns next available record location
     *
     * @return first available record location if found, otherwise the next lowest record or change location
     */
    public int getNextAvailableLocation() {
        return getRecords().stream().filter(LinkDBRecord::isAvailable).map(LinkDBRecord::getLocation).findFirst()
                .orElse(Math.min(getLastRecordLocation(), getLastPendingChangeLocation() - RECORD_BYTE_SIZE));
    }

    /**
     * Returns database read/write mode
     *
     * @return read/write mode based on device insteon engine
     */
    public ReadWriteMode getReadWriteMode() {
        switch (device.getInsteonEngine()) {
            case I1:
            case I2:
                return ReadWriteMode.PEEK_POKE;
            case I2CS:
                return ReadWriteMode.STANDARD;
            default:
                return ReadWriteMode.UNKNOWN;
        }
    }

    /**
     * Clears this link db
     */
    public synchronized void clear() {
        if (logger.isDebugEnabled()) {
            logger.debug("clearing link db for {}", device.getAddress());
        }
        records.clear();
        changes.clear();
        status = DatabaseStatus.EMPTY;
        delta = -1;
        reload = false;
        update = false;
    }

    /**
     * Loads this link db
     */
    public void load() {
        load(0L);
    }

    /**
     * Loads this link db with a delay
     *
     * @param delay reading delay (in milliseconds)
     */
    public void load(long delay) {
        DatabaseManager dbm = getDatabaseManager();
        if (!device.isAwake() || !device.isResponding()) {
            if (logger.isDebugEnabled()) {
                logger.debug("deferring load link db for {}, device is not awake or responding", device.getAddress());
            }
            setReload(true);
        } else if (dbm == null) {
            if (logger.isDebugEnabled()) {
                logger.debug("unable to load link db for {}, database manager not available", device.getAddress());
            }
        } else {
            clear();
            setStatus(DatabaseStatus.LOADING);
            dbm.read(device, delay);
        }
    }

    /**
     * Updates this link db with pending changes
     */
    public void update() {
        update(0L);
    }

    /**
     * Updates this link db with pending changes and a delay
     *
     * @param delay writing delay (in milliseconds)
     */
    public void update(long delay) {
        DatabaseManager dbm = getDatabaseManager();
        if (getPendingChanges().isEmpty()) {
            if (logger.isDebugEnabled()) {
                logger.debug("no pending changes to update link db for {}", device.getAddress());
            }
            setUpdate(false);
        } else if (!device.isAwake() || !device.isResponding()) {
            if (logger.isDebugEnabled()) {
                logger.debug("deferring update link db for {}, device is not awake or responding", device.getAddress());
            }
            setUpdate(true);
        } else if (dbm == null) {
            if (logger.isDebugEnabled()) {
                logger.debug("unable to update link db for {}, database manager not available", device.getAddress());
            }
        } else {
            dbm.write(device, delay);
        }
    }

    /**
     * Adds a link db record
     *
     * @param record the record to add
     * @return the previous record if overwritten
     */
    public @Nullable LinkDBRecord addRecord(LinkDBRecord record) {
        synchronized (records) {
            LinkDBRecord prevRecord = records.put(record.getLocation(), record);
            // move last record if overwritten
            if (prevRecord != null && prevRecord.isLast()) {
                int location = prevRecord.getLocation() - RECORD_BYTE_SIZE;
                records.put(location, LinkDBRecord.withNewLocation(location, prevRecord));
                if (logger.isTraceEnabled()) {
                    logger.trace("moved last record for {} to location {}", device.getAddress(),
                            HexUtils.getHexString(location));
                }
            }
            return prevRecord;
        }
    }

    /**
     * Loads a list of link db records
     *
     * @param records list of records to load
     */
    public void loadRecords(List<LinkDBRecord> records) {
        if (logger.isTraceEnabled()) {
            logger.trace("loading link db records for {}", device.getAddress());
        }
        records.forEach(this::addRecord);
        recordsLoaded();
    }

    /**
     * Logs the link db records
     */
    private void logRecords() {
        if (logger.isDebugEnabled()) {
            if (getRecords().isEmpty()) {
                logger.debug("no link records found for {}", device.getAddress());
            } else {
                logger.debug("---------------- start of link records for {} ----------------", device.getAddress());
                getRecords().stream().map(String::valueOf).forEach(logger::debug);
                logger.debug("----------------- end of link records for {} -----------------", device.getAddress());
            }
        }
    }

    /**
     * Notifies that the link db records have been loaded
     */
    public void recordsLoaded() {
        logRecords();
        updateStatus();
        device.linkDBUpdated();
    }

    /**
     * Clears the link db pending changes
     */
    public void clearPendingChanges() {
        if (logger.isDebugEnabled()) {
            logger.debug("clearing link db pending changes for {}", device.getAddress());
        }
        synchronized (changes) {
            changes.clear();
        }
    }

    /**
     * Adds a link db pending change
     *
     * @param record the pending change record to add
     */
    private void addPendingChange(LinkDBRecord record) {
        synchronized (changes) {
            LinkDBRecord prevRecord = changes.put(record.getLocation(), record);
            if (prevRecord == null) {
                if (logger.isTraceEnabled()) {
                    logger.trace("added change record: {}", record);
                }
            } else {
                if (logger.isTraceEnabled()) {
                    logger.trace("modified change record from: {} to: {}", prevRecord, record);
                }
            }
        }
    }

    /**
     * Marks a link db record for add
     *
     * @param record the record to add
     */
    public void markRecordForAdd(LinkDBRecord record) {
        int location = getPendingChangeLocation(record.getAddress(), record.getGroup(), record.isController(),
                record.getComponentId());
        addPendingChange(LinkDBRecord.withNewLocation(location, record));
    }

    /**
     * Marks a link db record for add
     *
     * @param address the record address to add
     * @param group the record group to add
     * @param isController if is controller record
     * @param data the record data to add
     */
    public void markRecordForAdd(InsteonAddress address, int group, boolean isController, byte[] data) {
        int location = getPendingChangeLocation(address, group, isController, data[2]);
        addPendingChange(LinkDBRecord.create(location, address, group, isController, data));
    }

    /**
     * Marks a link db record for delete
     *
     * @param record the record to be deleted
     */
    public void markRecordForDelete(LinkDBRecord record) {
        if (record.isAvailable()) {
            if (logger.isDebugEnabled()) {
                logger.debug("ignoring already deleted record: {}", record);
            }
            return;
        }
        RecordType type = RecordType.asInactive(record.getFlags());
        addPendingChange(LinkDBRecord.withNewType(type, record));
    }

    /**
     * Marks a link db record for delete
     *
     * @param address the record address to delete
     * @param group the record group to delete
     * @param isController if is controller record
     * @param componentId the record componentId to delete
     */
    public void markRecordForDelete(InsteonAddress address, int group, boolean isController, int componentId) {
        LinkDBRecord record = getActiveRecord(address, group, isController, componentId);
        if (record == null) {
            if (logger.isDebugEnabled()) {
                logger.debug("no active record found for {} group:{} isController:{} componentId:{}", address, group,
                        isController, HexUtils.getHexString(componentId));
            }
            return;
        }
        markRecordForDelete(record);
    }

    /**
     * Updates link database delta
     *
     * @param newDelta the database delta to update to
     */
    public void updateDatabaseDelta(int newDelta) {
        int oldDelta = getDatabaseDelta();
        // ignore delta if not defined or equal to old one
        if (newDelta == -1 || oldDelta == newDelta) {
            return;
        }
        // set database delta
        setDatabaseDelta(newDelta);
        // set db to reload if old delta defined and less than new one
        if (oldDelta != -1 && oldDelta < newDelta) {
            setReload(true);
        }
    }

    /**
     * Updates link database status
     */
    public synchronized void updateStatus() {
        if (records.isEmpty()) {
            if (logger.isDebugEnabled()) {
                logger.debug("no link db records for {}", device.getAddress());
            }
            setStatus(DatabaseStatus.EMPTY);
            return;
        }

        int firstLocation = records.firstKey();
        int lastLocation = records.lastKey();
        int expected = (firstLocation - lastLocation) / RECORD_BYTE_SIZE + 1;
        if (firstLocation != getFirstRecordLocation()) {
            if (logger.isDebugEnabled()) {
                logger.debug("got unexpected first record location for {}", device.getAddress());
            }
            setStatus(DatabaseStatus.PARTIAL);
        } else if (!records.lastEntry().getValue().isLast()) {
            if (logger.isDebugEnabled()) {
                logger.debug("got unexpected last record type for {}", device.getAddress());
            }
            setStatus(DatabaseStatus.PARTIAL);
        } else if (records.size() != expected) {
            if (logger.isDebugEnabled()) {
                logger.debug("got {} records for {} expected {}", records.size(), device.getAddress(), expected);
            }
            setStatus(DatabaseStatus.PARTIAL);
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("got complete link db records ({}) for {} ", records.size(), device.getAddress());
            }
            setStatus(DatabaseStatus.COMPLETE);
        }
    }

    /**
     * Returns broadcast group for a given component id
     *
     * @param componentId the record data3 field
     * @return list of the broadcast groups
     */
    public List<Integer> getBroadcastGroups(int componentId) {
        List<Integer> groups = List.of();
        InsteonModem modem = getModem();
        if (modem != null) {
            // unique groups from modem responder records matching component id and on level > 0
            groups = getRecords().stream()
                    .filter(record -> record.isActive() && record.isResponder()
                            && record.getAddress().equals(modem.getAddress()) && record.getComponentId() == componentId
                            && record.getOnLevel() > 0)
                    .map(LinkDBRecord::getGroup).filter(InsteonScene::isValidGroup).map(Integer::valueOf).distinct()
                    .toList();
        }
        return groups;
    }

    /**
     * Returns a list of related devices for a given group
     *
     * @param group the record group
     * @return list of related device addresses
     */
    public List<InsteonAddress> getRelatedDevices(int group) {
        List<InsteonAddress> devices = List.of();
        InsteonModem modem = getModem();
        if (modem != null) {
            // unique addresses from controller records matching group and is in modem database
            devices = getRecords().stream()
                    .filter(record -> record.isActive() && record.isController() && record.getGroup() == group
                            && modem.getDB().hasEntry(record.getAddress()))
                    .map(LinkDBRecord::getAddress).distinct().toList();
        }
        return devices;
    }
}
