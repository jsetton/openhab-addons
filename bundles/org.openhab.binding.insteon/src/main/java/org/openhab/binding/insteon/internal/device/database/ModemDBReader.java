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

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.eclipse.jdt.annotation.NonNullByDefault;
import org.eclipse.jdt.annotation.Nullable;
import org.openhab.binding.insteon.internal.device.InsteonAddress;
import org.openhab.binding.insteon.internal.device.InsteonModem;
import org.openhab.binding.insteon.internal.device.ProductData;
import org.openhab.binding.insteon.internal.transport.PortListener;
import org.openhab.binding.insteon.internal.transport.message.FieldException;
import org.openhab.binding.insteon.internal.transport.message.InvalidMessageTypeException;
import org.openhab.binding.insteon.internal.transport.message.Msg;
import org.openhab.binding.insteon.internal.transport.message.Priority;
import org.openhab.binding.insteon.internal.utils.HexUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link ModemDBReader} manages modem database read requests
 *
 * @author Jeremy Setton - Initial contribution
 */
@NonNullByDefault
public class ModemDBReader implements PortListener {
    private static final int PRODUCT_REQUEST_RETRIES = 2;
    private static final int PRODUCT_REQUEST_TIMEOUT = 3000; // in milliseconds

    private static enum ReaderStatus {
        LOADING_RECORDS,
        LOADING_PRODUCTS,
        DONE
    }

    private final Logger logger = LoggerFactory.getLogger(ModemDBReader.class);

    private InsteonModem modem;
    private ScheduledExecutorService scheduler;
    private @Nullable ScheduledFuture<?> restartJob;
    private @Nullable ScheduledFuture<?> productJob;
    private Queue<InsteonAddress> productQueue = new ConcurrentLinkedQueue<>();
    private ReaderStatus status = ReaderStatus.DONE;
    private volatile long lastMsgReceived;
    private volatile int messageCount;
    private volatile int retryCount;

    public ModemDBReader(InsteonModem modem, ScheduledExecutorService scheduler) {
        this.modem = modem;
        this.scheduler = scheduler;

        modem.getPort().registerListener(this);
    }

    public boolean isRunning() {
        return status != ReaderStatus.DONE;
    }

    public void read() {
        logger.debug("starting modem database reader");

        getAllRecords();
    }

    public void stop() {
        logger.debug("modem database reader finished");

        ScheduledFuture<?> restartJob = this.restartJob;
        if (restartJob != null) {
            restartJob.cancel(true);
            this.restartJob = null;
        }

        ScheduledFuture<?> productJob = this.productJob;
        if (productJob != null) {
            productJob.cancel(true);
            this.productJob = null;
        }

        modem.getDBM().operationCompleted();
    }

    private void restart() {
        lastMsgReceived = System.currentTimeMillis();
        messageCount = 0;

        switch (status) {
            case LOADING_RECORDS:
                modem.reconnect();
                getAllRecords();
                break;
            case LOADING_PRODUCTS:
                modem.reconnect();
                getAllProductData();
                break;
            case DONE:
                logger.trace("reader already done, ignoring restart");
                stop();
                break;
        }
    }

    private void getAllRecords() {
        status = ReaderStatus.LOADING_RECORDS;
        modem.getDB().clear();
        getFirstLinkRecord();
    }

    private void getAllProductData() {
        status = ReaderStatus.LOADING_PRODUCTS;
        productQueue.clear();
        productQueue.addAll(modem.getDB().getDevices());
        getNextProductData();
    }

    private void done() {
        modem.getDB().recordsLoaded();
        modem.getDB().setIsComplete(true);
        status = ReaderStatus.DONE;
        stop();
    }

    private void getFirstLinkRecord() {
        try {
            Msg msg = Msg.makeMessage("GetFirstALLLinkRecord");
            msg.setPriority(Priority.DATABASE);
            modem.writeMessage(msg);
        } catch (InvalidMessageTypeException e) {
            logger.warn("error creating message", e);
        } catch (IOException e) {
            logger.warn("error sending first link record query", e);
        }
    }

    private void getNextLinkRecord() {
        try {
            Msg msg = Msg.makeMessage("GetNextALLLinkRecord");
            msg.setPriority(Priority.DATABASE);
            modem.writeMessage(msg);
        } catch (InvalidMessageTypeException e) {
            logger.warn("error creating message", e);
        } catch (IOException e) {
            logger.warn("error sending next link record query", e);
        }
    }

    private void getProductId(InsteonAddress address) {
        try {
            Msg msg = Msg.makeStandardMessage(address, (byte) 0x10, (byte) 0x00);
            msg.setPriority(Priority.DATABASE);
            modem.writeMessage(msg);
        } catch (FieldException | InvalidMessageTypeException e) {
            logger.warn("error creating message", e);
        } catch (IOException e) {
            logger.warn("error sending product id query", e);
        }
    }

    private void getProductData(InsteonAddress address) {
        // skip if reader status not done, device not in modem db or product data already known
        if (status != ReaderStatus.DONE || !modem.getDB().hasEntry(address) || modem.getDB().hasProductData(address)) {
            return;
        }
        // add product address if not already queued
        if (!productQueue.contains(address)) {
            productQueue.add(address);
        }
        // get product data if not running already
        if (productJob == null) {
            getNextProductData();
        }
    }

    private void getNextProductData() {
        ScheduledFuture<?> productJob = this.productJob;
        if (productJob != null) {
            productJob.cancel(true);
            this.productJob = null;
        }

        InsteonAddress address = productQueue.peek();
        if (address != null) {
            getProductId(address);
        } else if (status == ReaderStatus.LOADING_PRODUCTS) {
            logger.debug("got all product data");
            done();
        }
    }

    private void startProductRequestTimer(Msg msg) throws FieldException {
        InsteonAddress address = msg.getInsteonAddress("toAddress");
        logger.trace("starting product request timer for {}", address);

        retryCount = 0;

        productJob = scheduler.scheduleWithFixedDelay(() -> {
            if (productQueue.contains(address)) {
                if (retryCount++ < PRODUCT_REQUEST_RETRIES) {
                    logger.trace("product request retry #{} for {}", retryCount, address);
                    getProductId(address);
                } else if (productQueue.remove(address)) {
                    logger.debug("product request failed for {}", address);
                    getNextProductData();
                }
            }
        }, PRODUCT_REQUEST_TIMEOUT, PRODUCT_REQUEST_TIMEOUT, TimeUnit.MILLISECONDS);
    }

    private void startRestartTimer() {
        logger.trace("starting restart timer");

        lastMsgReceived = System.currentTimeMillis();
        messageCount = 0;

        restartJob = scheduler.scheduleWithFixedDelay(() -> {
            if (System.currentTimeMillis() - lastMsgReceived > DatabaseManager.MESSAGE_TIMEOUT) {
                String s = "";
                if (messageCount == 0) {
                    s = """
                            No messages were received, the PLM or hub might be broken. If this continues see \
                            'Known Limitations and Issues' in the Insteon binding documentation.\
                            """;
                }
                logger.warn("Failed to read modem database, restarting!{}", s);
                restart();
            }
        }, 0, 1000, TimeUnit.MILLISECONDS);
    }

    @Override
    public void disconnected() {
        if (status != ReaderStatus.DONE) {
            logger.debug("port disconnected, restarting");
            restart();
        }
    }

    @Override
    public void messageReceived(Msg msg) {
        if (status != ReaderStatus.DONE) {
            lastMsgReceived = msg.getTimestamp();
            messageCount++;
        }

        try {
            if (msg.getCommand() == 0x50 && (msg.isAllLinkCleanup() || msg.isAllLinkSuccessReport())) {
                // we got an all link cleanup or success report message
                handleAllLinkMessage(msg);
            } else if (msg.getCommand() == 0x50 && msg.isBroadcast()
                    && (msg.getByte("command1") == 0x01 || msg.getByte("command1") == 0x02)) {
                // we got a product data broadcast message
                handleProductData(msg);
            } else if (msg.getCommand() == 0x53) {
                // we got a linking completed message
                handleLinkingCompleted(msg);
            } else if (msg.getCommand() == 0x57) {
                // we got a link record response
                handleLinkRecord(msg);
            } else if ((msg.getCommand() == 0x69 || msg.getCommand() == 0x6A) && msg.isReplyNack()) {
                // we got a get link record reply nack
                if (status == ReaderStatus.LOADING_RECORDS) {
                    logger.debug("got all link records");
                    getAllProductData();
                }
            } else if (msg.getCommand() == 0x6F && msg.isReplyAck()) {
                // we got a manage link record reply ack
                handleLinkRecordUpdated(msg);
            }
        } catch (FieldException e) {
            logger.warn("error parsing message", e);
        }
    }

    @Override
    public void messageSent(Msg msg) {
        try {
            if (msg.getCommand() == 0x62 && msg.getByte("command1") == 0x10) {
                // we sent a get product id message
                if (status != ReaderStatus.LOADING_RECORDS && productJob == null) {
                    startProductRequestTimer(msg);
                }
            } else if (msg.getCommand() == 0x69) {
                // we sent a get first link record message
                if (status == ReaderStatus.LOADING_RECORDS && restartJob == null) {
                    startRestartTimer();
                }
            }
        } catch (FieldException e) {
            logger.warn("error parsing message", e);
        }
    }

    private void handleLinkRecord(Msg msg) throws FieldException {
        if (status != ReaderStatus.LOADING_RECORDS) {
            logger.debug("unsolicited link record, ignoring");
            return;
        }
        ModemDBRecord record = ModemDBRecord.fromRecordMsg(msg);
        modem.getDB().addRecord(record);
        getNextLinkRecord();
    }

    private void handleLinkRecordUpdated(Msg msg) throws FieldException {
        ModemDBRecord record = ModemDBRecord.fromRecordMsg(msg);
        InsteonAddress address = msg.getInsteonAddress("LinkAddr");
        int group = msg.getInt("ALLLinkGroup");
        int code = msg.getInt("ControlCode");
        ManageRecordAction action = ManageRecordAction.valueOf(code);
        switch (action) {
            case MODIFY_OR_ADD:
                modem.getDB().modifyOrAddRecord(record);
                break;
            case MODIFY_CONTROLLER_OR_ADD:
                modem.getDB().modifyOrAddControllerRecord(record);
                break;
            case MODIFY_RESPONDER_OR_ADD:
                modem.getDB().modifyOrAddResponderRecord(record);
                break;
            case DELETE:
                modem.getDB().deleteRecord(address, group);
                break;
            default:
                logger.debug("got invalid control code: {}", HexUtils.getHexString(code));
                return;
        }
        modem.getDB().linkUpdated(address, group, false);
        getProductData(address);
    }

    private void handleLinkingCompleted(Msg msg) throws FieldException {
        ModemDBRecord record = ModemDBRecord.fromLinkingMsg(msg);
        InsteonAddress address = msg.getInsteonAddress("LinkAddr");
        int group = msg.getInt("ALLLinkGroup");
        int code = msg.getInt("LinkCode");
        LinkMode mode = LinkMode.valueOf(code);
        switch (mode) {
            case CONTROLLER:
                modem.getDB().modifyOrAddControllerRecord(record);
                break;
            case RESPONDER:
                modem.getDB().modifyOrAddResponderRecord(record);
                break;
            case DELETE:
                modem.getDB().deleteRecord(address, group);
                break;
            default:
                logger.debug("got invalid link code: {}", HexUtils.getHexString(code));
                return;
        }
        modem.getDB().linkUpdated(address, group, true);
        getProductData(address);
    }

    private void handleAllLinkMessage(Msg msg) throws FieldException {
        InsteonAddress address = msg.getInsteonAddress("fromAddress");
        getProductData(address);
    }

    private void handleProductData(Msg msg) throws FieldException {
        InsteonAddress fromAddr = msg.getInsteonAddress("fromAddress");
        InsteonAddress toAddr = msg.getInsteonAddress("toAddress");
        int deviceCategory = Byte.toUnsignedInt(toAddr.getHighByte());
        int subCategory = Byte.toUnsignedInt(toAddr.getMiddleByte());
        int firmware = Byte.toUnsignedInt(toAddr.getLowByte());
        int hardware = msg.getInt("command2");
        ProductData productData = ProductData.makeInsteonProduct(deviceCategory, subCategory);
        productData.setFirmwareVersion(firmware);
        productData.setHardwareVersion(hardware);
        // set product data if in modem db
        if (modem.getDB().hasEntry(fromAddr)) {
            modem.getDB().setProductData(fromAddr, productData);
        }
        // get next product data if in queue
        if (productQueue.remove(fromAddr)) {
            getNextProductData();
        }
    }
}
