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
package org.openhab.binding.insteon.internal;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;

import javax.xml.parsers.ParserConfigurationException;

import org.eclipse.jdt.annotation.NonNullByDefault;
import org.eclipse.jdt.annotation.Nullable;
import org.openhab.binding.insteon.internal.config.InsteonLegacyChannelConfiguration;
import org.openhab.binding.insteon.internal.config.InsteonLegacyNetworkConfiguration;
import org.openhab.binding.insteon.internal.device.DeviceAddress;
import org.openhab.binding.insteon.internal.device.InsteonAddress;
import org.openhab.binding.insteon.internal.device.LegacyDevice;
import org.openhab.binding.insteon.internal.device.LegacyDevice.DeviceStatus;
import org.openhab.binding.insteon.internal.device.LegacyDeviceFeature;
import org.openhab.binding.insteon.internal.device.LegacyDeviceType;
import org.openhab.binding.insteon.internal.device.LegacyDeviceTypeLoader;
import org.openhab.binding.insteon.internal.device.LegacyPollManager;
import org.openhab.binding.insteon.internal.device.LegacyRequestManager;
import org.openhab.binding.insteon.internal.device.X10Address;
import org.openhab.binding.insteon.internal.device.database.LegacyModemDBEntry;
import org.openhab.binding.insteon.internal.device.feature.LegacyFeatureListener;
import org.openhab.binding.insteon.internal.handler.InsteonLegacyNetworkHandler;
import org.openhab.binding.insteon.internal.transport.LegacyDriver;
import org.openhab.binding.insteon.internal.transport.LegacyDriverListener;
import org.openhab.binding.insteon.internal.transport.LegacyPort;
import org.openhab.binding.insteon.internal.transport.LegacyPortListener;
import org.openhab.binding.insteon.internal.transport.message.FieldException;
import org.openhab.binding.insteon.internal.transport.message.Msg;
import org.openhab.core.io.transport.serial.SerialPortManager;
import org.openhab.core.thing.ChannelUID;
import org.openhab.core.types.Command;
import org.openhab.core.types.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

/**
 * A majority of the code in this file is from the openHAB 1 binding
 * org.openhab.binding.insteonplm.InsteonPLMActiveBinding. Including the comments below.
 *
 * -----------------------------------------------------------------------------------------------
 *
 * This class represents the actual implementation of the binding, and controls the high level flow
 * of messages to and from the InsteonModem.
 *
 * Writing this binding has been an odyssey through the quirks of the Insteon protocol
 * and Insteon devices. A substantial redesign was necessary at some point along the way.
 * Here are some of the hard learned lessons that should be considered by anyone who wants
 * to re-architect the binding:
 *
 * 1) The entries of the link database of the modem are not reliable. The category/subcategory entries in
 * particular have junk data. Forget about using the modem database to generate a list of devices.
 * The database should only be used to verify that a device has been linked.
 *
 * 2) Querying devices for their product information does not work either. First of all, battery operated devices
 * (and there are a lot of those) have their radio switched off, and may generally not respond to product
 * queries. Even main stream hardwired devices sold presently (like the 2477s switch and the 2477d dimmer)
 * don't even have a product ID. Although supposedly part of the Insteon protocol, we have yet to
 * encounter a device that would cough up a product id when queried, even among very recent devices. They
 * simply return zeros as product id. Lesson: forget about querying devices to generate a device list.
 *
 * 3) Polling is a thorny issue: too much traffic on the network, and messages will be dropped left and right,
 * and not just the poll related ones, but others as well. In particular sending back-to-back messages
 * seemed to result in the second message simply never getting sent, without flow control back pressure
 * (NACK) from the modem. For now the work-around is to space out the messages upon sending, and
 * in general poll as infrequently as acceptable.
 *
 * 4) Instantiating and tracking devices when reported by the modem (either from the database, or when
 * messages are received) leads to complicated state management because there is no guarantee at what
 * point (if at all) the binding configuration will be available. It gets even more difficult when
 * items are created, destroyed, and modified while the binding runs.
 *
 * For the above reasons, devices are only instantiated when they are referenced by binding information.
 * As nice as it would be to discover devices and their properties dynamically, we have abandoned that
 * path because it had led to a complicated and fragile system which due to the technical limitations
 * above was inherently squirrely.
 *
 *
 * @author Bernd Pfrommer - Initial contribution
 * @author Daniel Pfrommer - openHAB 1 insteonplm binding
 * @author Rob Nielsen - Port to openHAB 2 insteon binding
 * @author Jeremy Setton - Rewrite insteon binding
 */
@NonNullByDefault
public class InsteonLegacyBinding implements LegacyDriverListener, LegacyPortListener {
    private static final int DEAD_DEVICE_COUNT = 10;

    private final Logger logger = LoggerFactory.getLogger(InsteonLegacyBinding.class);

    private LegacyDriver driver;
    private Map<DeviceAddress, LegacyDevice> devices = new ConcurrentHashMap<>();
    private Map<String, InsteonLegacyChannelConfiguration> bindingConfigs = new ConcurrentHashMap<>();
    private int devicePollIntervalMilliseconds = 300000;
    private int deadDeviceTimeout = -1;
    private boolean driverInitialized = false;
    private int messagesReceived = 0;
    private boolean isActive = false; // state of binding
    private int x10HouseUnit = -1;
    private InsteonLegacyNetworkHandler handler;

    public InsteonLegacyBinding(InsteonLegacyNetworkHandler handler, InsteonLegacyNetworkConfiguration config,
            SerialPortManager serialPortManager, ScheduledExecutorService scheduler) {
        this.handler = handler;

        String port = config.getRedactedPort();
        logger.debug("port = '{}'", port);

        driver = new LegacyDriver(config, this, serialPortManager, scheduler);
        driver.addPortListener(this);

        Integer devicePollIntervalSeconds = config.getDevicePollIntervalSeconds();
        if (devicePollIntervalSeconds != null) {
            devicePollIntervalMilliseconds = devicePollIntervalSeconds * 1000;
        }
        logger.debug("device poll interval set to {} seconds", devicePollIntervalMilliseconds / 1000);

        String additionalDevices = config.getAdditionalDevices();
        if (additionalDevices != null) {
            try {
                LegacyDeviceTypeLoader instance = LegacyDeviceTypeLoader.instance();
                if (instance != null) {
                    instance.loadDeviceTypesXML(additionalDevices);
                    logger.debug("read additional device definitions from {}", additionalDevices);
                } else {
                    logger.warn("device type loader instance is null");
                }
            } catch (ParserConfigurationException | SAXException | IOException e) {
                logger.warn("error reading additional devices from {}", additionalDevices, e);
            }
        }

        String additionalFeatures = config.getAdditionalFeatures();
        if (additionalFeatures != null) {
            logger.debug("reading additional feature templates from {}", additionalFeatures);
            LegacyDeviceFeature.readFeatureTemplates(additionalFeatures);
        }

        deadDeviceTimeout = devicePollIntervalMilliseconds * DEAD_DEVICE_COUNT;
        logger.debug("dead device timeout set to {} seconds", deadDeviceTimeout / 1000);
    }

    public LegacyDriver getDriver() {
        return driver;
    }

    public boolean isDriverInitialized() {
        return driverInitialized;
    }

    public boolean startPolling() {
        logger.debug("starting to poll {}", driver.getPortName());
        driver.start();
        return driver.isRunning();
    }

    public void setIsActive(boolean isActive) {
        this.isActive = isActive;
    }

    public void sendCommand(String channelName, Command command) {
        if (!isActive) {
            logger.debug("not ready to handle commands yet, returning.");
            return;
        }

        InsteonLegacyChannelConfiguration bindingConfig = bindingConfigs.get(channelName);
        if (bindingConfig == null) {
            logger.warn("unable to find binding config for channel {}", channelName);
            return;
        }

        LegacyDevice dev = getDevice(bindingConfig.getAddress());
        if (dev == null) {
            logger.warn("no device found with insteon address {}", bindingConfig.getAddress());
            return;
        }

        dev.processCommand(driver, bindingConfig, command);

        logger.debug("found binding config for channel {}", channelName);
    }

    public void addFeatureListener(InsteonLegacyChannelConfiguration bindingConfig) {
        logger.debug("adding listener for channel {}", bindingConfig.getChannelName());

        DeviceAddress address = bindingConfig.getAddress();
        LegacyDevice dev = getDevice(address);
        if (dev == null) {
            logger.warn("device for address {} is null", address);
            return;
        }
        @Nullable
        LegacyDeviceFeature f = dev.getFeature(bindingConfig.getFeature());
        if (f == null || f.isFeatureGroup()) {
            StringBuilder buf = new StringBuilder();
            ArrayList<String> names = new ArrayList<>(dev.getFeatures().keySet());
            Collections.sort(names);
            for (String name : names) {
                LegacyDeviceFeature feature = dev.getFeature(name);
                if (feature != null && !feature.isFeatureGroup()) {
                    if (buf.length() > 0) {
                        buf.append(", ");
                    }
                    buf.append(name);
                }
            }

            logger.warn("channel {} references unknown feature: {}, it will be ignored. Known features for {} are: {}.",
                    bindingConfig.getChannelName(), bindingConfig.getFeature(), bindingConfig.getProductKey(),
                    buf.toString());
            return;
        }

        LegacyFeatureListener fl = new LegacyFeatureListener(this, bindingConfig.getChannelUID(),
                bindingConfig.getChannelName());
        fl.setParameters(bindingConfig.getParameters());
        f.addListener(fl);

        bindingConfigs.put(bindingConfig.getChannelName(), bindingConfig);
    }

    public void removeFeatureListener(ChannelUID channelUID) {
        String channelName = channelUID.getAsString();

        logger.debug("removing listener for channel {}", channelName);

        for (Iterator<Entry<DeviceAddress, LegacyDevice>> it = devices.entrySet().iterator(); it.hasNext();) {
            LegacyDevice dev = it.next().getValue();
            boolean removedListener = dev.removeFeatureListener(channelName);
            if (removedListener) {
                logger.trace("removed feature listener {} from dev {}", channelName, dev);
            }
        }
    }

    public void updateFeatureState(ChannelUID channelUID, State state) {
        handler.updateState(channelUID, state);
    }

    public @Nullable LegacyDevice makeNewDevice(DeviceAddress addr, String productKey,
            Map<String, Object> deviceConfigMap) {
        LegacyDeviceTypeLoader instance = LegacyDeviceTypeLoader.instance();
        if (instance == null) {
            return null;
        }
        LegacyDeviceType dt = instance.getDeviceType(productKey);
        if (dt == null) {
            return null;
        }
        LegacyDevice dev = LegacyDevice.makeDevice(dt);
        dev.setAddress(addr);
        dev.setProductKey(productKey);
        dev.setDriver(driver);
        dev.setIsModem(productKey.equals(InsteonLegacyBindingConstants.PLM_PRODUCT_KEY));
        dev.setDeviceConfigMap(deviceConfigMap);
        if (!dev.hasValidPollingInterval()) {
            dev.setPollInterval(devicePollIntervalMilliseconds);
        }
        if (driver.isModemDBComplete() && dev.getStatus() != DeviceStatus.POLLING) {
            int ndev = checkIfInModemDatabase(dev);
            if (dev.hasModemDBEntry()) {
                dev.setStatus(DeviceStatus.POLLING);
                LegacyPollManager.instance().startPolling(dev, ndev);
            }
        }
        devices.put(addr, dev);

        handler.insteonDeviceWasCreated();

        return (dev);
    }

    public void removeDevice(DeviceAddress addr) {
        LegacyDevice dev = devices.remove(addr);
        if (dev == null) {
            return;
        }

        if (dev.getStatus() == DeviceStatus.POLLING) {
            LegacyPollManager.instance().stopPolling(dev);
        }
    }

    /**
     * Checks if a device is in the modem link database, and, if the database
     * is complete, logs a warning if the device is not present
     *
     * @param dev The device to search for in the modem database
     * @return number of devices in modem database
     */
    private int checkIfInModemDatabase(LegacyDevice dev) {
        try {
            Map<InsteonAddress, LegacyModemDBEntry> dbes = driver.lockModemDBEntries();
            if (dev.getAddress() instanceof InsteonAddress addr) {
                if (dbes.containsKey(addr)) {
                    if (!dev.hasModemDBEntry()) {
                        logger.debug("device {} found in the modem database and {}.", addr,
                                getLinkInfo(dbes, addr, true));
                        dev.setHasModemDBEntry(true);
                    }
                } else {
                    if (driver.isModemDBComplete()) {
                        logger.warn("device {} not found in the modem database. Did you forget to link?", addr);
                        handler.deviceNotLinked(addr);
                    }
                }
            }
            return dbes.size();
        } finally {
            driver.unlockModemDBEntries();
        }
    }

    public Map<String, String> getDatabaseInfo() {
        try {
            Map<String, String> databaseInfo = new HashMap<>();
            Map<InsteonAddress, LegacyModemDBEntry> dbes = driver.lockModemDBEntries();
            for (InsteonAddress addr : dbes.keySet()) {
                String a = addr.toString();
                databaseInfo.put(a, a + ": " + getLinkInfo(dbes, addr, false));
            }

            return databaseInfo;
        } finally {
            driver.unlockModemDBEntries();
        }
    }

    public boolean reconnect() {
        driver.stop();
        return startPolling();
    }

    /**
     * Everything below was copied from Insteon PLM v1
     */

    /**
     * Clean up all state.
     */
    public void shutdown() {
        logger.debug("shutting down Insteon bridge");
        driver.stop();
        devices.clear();
        LegacyRequestManager.destroyInstance();
        LegacyPollManager.instance().stop();
        isActive = false;
    }

    /**
     * Method to find a device by address
     *
     * @param aAddr the insteon address to search for
     * @return reference to the device, or null if not found
     */
    public @Nullable LegacyDevice getDevice(@Nullable DeviceAddress aAddr) {
        LegacyDevice dev = (aAddr == null) ? null : devices.get(aAddr);
        return (dev);
    }

    private String getLinkInfo(Map<InsteonAddress, LegacyModemDBEntry> dbes, InsteonAddress a, boolean prefix) {
        LegacyModemDBEntry dbe = dbes.get(a);
        if (dbe == null) {
            return "";
        }
        List<Byte> controls = dbe.getControls();
        List<Byte> responds = dbe.getRespondsTo();

        LegacyPort port = dbe.getPort();
        if (port == null) {
            return "";
        }
        String portName = port.getName();
        String s = portName.startsWith("/hub") ? "hub" : "plm";
        StringBuilder buf = new StringBuilder();
        if (port.isModem(a)) {
            if (prefix) {
                buf.append("it is the ");
            }
            buf.append(s);
            buf.append(" (");
            buf.append(portName);
            buf.append(")");
        } else {
            if (prefix) {
                buf.append("the ");
            }
            buf.append(s);
            buf.append(" controls groups (");
            buf.append(toGroupString(controls));
            buf.append(") and responds to groups (");
            buf.append(toGroupString(responds));
            buf.append(")");
        }

        return buf.toString();
    }

    private String toGroupString(List<Byte> group) {
        List<Byte> sorted = new ArrayList<>(group);
        Collections.sort(sorted, new Comparator<>() {
            @Override
            public int compare(Byte b1, Byte b2) {
                int i1 = b1 & 0xFF;
                int i2 = b2 & 0xFF;
                return i1 < i2 ? -1 : i1 == i2 ? 0 : 1;
            }
        });

        StringBuilder buf = new StringBuilder();
        for (Byte b : sorted) {
            if (buf.length() > 0) {
                buf.append(",");
            }
            buf.append(b & 0xFF);
        }

        return buf.toString();
    }

    public void logDeviceStatistics() {
        String msg = String.format("devices: %3d configured, %3d polling, msgs received: %5d", devices.size(),
                LegacyPollManager.instance().getSizeOfQueue(), messagesReceived);
        logger.debug("{}", msg);
        messagesReceived = 0;
        for (LegacyDevice dev : devices.values()) {
            if (dev.isModem()) {
                continue;
            }
            if (deadDeviceTimeout > 0 && dev.getPollOverDueTime() > deadDeviceTimeout) {
                logger.debug("device {} has not responded to polls for {} sec", dev.toString(),
                        dev.getPollOverDueTime() / 3600);
            }
        }
    }

    @Override
    public void msg(Msg msg) {
        if (msg.isEcho() || msg.isPureNack()) {
            return;
        }
        messagesReceived++;
        logger.debug("got msg: {}", msg);
        try {
            if (msg.isX10()) {
                handleX10Message(msg);
            } else if (msg.isInsteon()) {
                handleInsteonMessage(msg);
            }
        } catch (FieldException e) {
            logger.warn("got bad message: {}", msg, e);
        }
    }

    @Override
    public void driverCompletelyInitialized() {
        List<InsteonAddress> missing = new ArrayList<>();
        try {
            Map<InsteonAddress, LegacyModemDBEntry> dbes = driver.lockModemDBEntries();
            logger.debug("modem database has {} entries!", dbes.size());
            if (dbes.isEmpty()) {
                logger.warn("the modem link database is empty!");
            }
            for (InsteonAddress k : dbes.keySet()) {
                logger.debug("modem db entry: {}", k);
            }
            Set<InsteonAddress> addrs = new HashSet<>();
            for (LegacyDevice dev : devices.values()) {
                if (dev.getAddress() instanceof InsteonAddress a) {
                    if (!dbes.containsKey(a)) {
                        logger.warn("device {} not found in the modem database. Did you forget to link?", a);
                        handler.deviceNotLinked(a);
                    } else {
                        if (!dev.hasModemDBEntry()) {
                            addrs.add(a);
                            logger.debug("device {} found in the modem database and {}.", a,
                                    getLinkInfo(dbes, a, true));
                            dev.setHasModemDBEntry(true);
                        }
                        if (dev.getStatus() != DeviceStatus.POLLING) {
                            LegacyPollManager.instance().startPolling(dev, dbes.size());
                        }
                    }
                }
            }

            for (InsteonAddress a : dbes.keySet()) {
                if (!addrs.contains(a)) {
                    logger.debug("device {} found in the modem database, but is not configured as a thing and {}.", a,
                            getLinkInfo(dbes, a, true));

                    missing.add(a);
                }
            }
        } finally {
            driver.unlockModemDBEntries();
        }

        if (!missing.isEmpty()) {
            handler.addMissingDevices(missing);
        }

        driverInitialized = true;
    }

    @Override
    public void disconnected() {
        handler.bindingDisconnected();
    }

    private void handleInsteonMessage(Msg msg) throws FieldException {
        InsteonAddress toAddr = msg.getInsteonAddress("toAddress");
        if (!msg.isBroadcast() && !driver.isMsgForUs(toAddr)) {
            // not for one of our modems, do not process
            return;
        }
        InsteonAddress fromAddr = msg.getInsteonAddress("fromAddress");
        handleMessage(fromAddr, msg);
    }

    private void handleX10Message(Msg msg) throws FieldException {
        int x10Flag = msg.getByte("X10Flag") & 0xff;
        int rawX10 = msg.getByte("rawX10") & 0xff;
        if (x10Flag == 0x80) { // actual command
            if (x10HouseUnit != -1) {
                X10Address fromAddr = new X10Address((byte) x10HouseUnit);
                handleMessage(fromAddr, msg);
            }
        } else if (x10Flag == 0) {
            // what unit the next cmd will apply to
            x10HouseUnit = rawX10 & 0xFF;
        }
    }

    private void handleMessage(DeviceAddress fromAddr, Msg msg) {
        LegacyDevice dev = getDevice(fromAddr);
        if (dev == null) {
            logger.debug("dropping message from unknown device with address {}", fromAddr);
        } else {
            dev.handleMessage(msg);
        }
    }
}
