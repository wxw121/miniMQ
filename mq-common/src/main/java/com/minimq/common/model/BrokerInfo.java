package com.minimq.common.model;

/**
 * Broker 基础信息值对象，用于在模块间传递 brokerId、host 和 port。
 */
public record BrokerInfo(int brokerId, String host, int port) {
}
