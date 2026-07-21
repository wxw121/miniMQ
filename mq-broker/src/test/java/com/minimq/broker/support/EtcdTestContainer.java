package com.minimq.broker.support;

import com.minimq.broker.coordination.EtcdClientFactory;
import io.etcd.jetcd.Client;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.List;

public final class EtcdTestContainer extends GenericContainer<EtcdTestContainer> {
    private static final DockerImageName IMAGE = DockerImageName.parse("quay.io/coreos/etcd:v3.5.21");

    public EtcdTestContainer() {
        super(IMAGE);
        withExposedPorts(2379);
        withCommand("etcd", "--name", "default", "--data-dir", "/etcd-data",
                "--listen-client-urls", "http://0.0.0.0:2379",
                "--advertise-client-urls", "http://0.0.0.0:2379",
                "--listen-peer-urls", "http://0.0.0.0:2380",
                "--initial-advertise-peer-urls", "http://0.0.0.0:2380",
                "--initial-cluster", "default=http://0.0.0.0:2380",
                "--initial-cluster-state", "new");
    }

    public String endpoint() {
        return "http://" + getHost() + ":" + getMappedPort(2379);
    }

    public Client newClient() {
        return EtcdClientFactory.create(List.of(endpoint()));
    }
}
