package org.apache.ignite.spi.discovery.tcp.ipfinder.nacos;
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.NamingFactory;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacos.common.utils.StringUtils;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.IgniteSpiConfiguration;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinderAdapter;

/**
 * Nacos based IP Finder
 * <p>
 * For information about Blob Storage visit <a href="https://azure.microsoft.com/en-in/services/storage/blobs/">azure.microsoft.com</a>.
 * <h1 class="header">Configuration</h1>
 * <h2 class="header">Mandatory</h2>
 * <ul>
 *      <li>IgniteServiceName (see {@link #setIgniteServiceName(String)})</li>
 *      <li>ServerAddr (see {@link #setServerAddr(String)})</li>
 *      <li>Namespace (see {@link #setNamespace(String)})</li>
 *      <li>DataId (see {@link #setDataId(String)})</li>
 *      <li>Group (see {@link #setGroup(String)})</li>
 * </ul>
 * <h2 class="header">Optional</h2>
 * <ul>
 *      <li>Shared flag (see {@link #setShared(boolean)})</li>
 * </ul>
 * <p>
 * The finder will create a container with the provided name. The container will contain entries named
 * like the following: {@code 192.168.1.136:1001}.
 * <p>
 * Note that storing data in Azure Blob Storage service will result in charges to your Azure account.
 * Choose another implementation of {@link org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder} for local
 * or home network tests.
 * <p>
 * Note that this finder is shared by default (see {@link org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder#isShared()}.
 */
public class TcpDiscoveryNacosIpFinder extends TcpDiscoveryIpFinderAdapter {

    /**
     * Init routine guard.
     */
    private final AtomicBoolean initGuard = new AtomicBoolean();
    /**
     * Init routine latch.
     */
    private final CountDownLatch initLatch = new CountDownLatch(1);
    /**
     * Grid logger.
     */
    @LoggerResource
    private IgniteLogger log;
    /**
     * nacos serverAddr
     */
    private String serverAddr;
    /**
     * nacos namespace
     */
    private String namespace;

    /**
     * nacos register preferredNetworks
     */
    private String preferredNetworks;

    /**
     * nacos dataId
     */
    private String dataId;
    /**
     * nacos group
     */
    private String group;
    /**
     * nacos group
     */
    private String igniteServiceName;
    /**
     * Storage credential
     */
    private NamingService namingService;

    /**
     * Default constructor
     */
    public TcpDiscoveryNacosIpFinder() {
        setShared(true);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<InetSocketAddress> getRegisteredAddresses() throws IgniteSpiException {
        init();

        Collection<InetSocketAddress> addrs = new ArrayList<>();
        Set<String> seenBlobNames = new HashSet<>();

        try {
            final List<Instance> instanceList = namingService.getAllInstances(igniteServiceName);
            for (Instance instance : instanceList) {
                addrs.add(new InetSocketAddress(instance.getIp(), instance.getPort()));
            }
        } catch (NacosException e) {
            throw new IgniteSpiException(
                    "Failed to get content from nacos", e);
        }

        return addrs;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void registerAddresses(Collection<InetSocketAddress> addrs) throws IgniteSpiException {
        assert !F.isEmpty(addrs);

        init();

        for (InetSocketAddress addr : addrs) {
            String key = keyFromAddr(addr);
            if (StringUtils.isNotBlank(preferredNetworks) && !key.startsWith(preferredNetworks)) {
                continue;
            }

            try {
                log.info("current server addr: " + key);
                namingService.registerInstance(igniteServiceName,
                        addr.getAddress().getHostAddress(), addr.getPort());
            } catch (NacosException e) {
                throw new IgniteSpiException("Failed to register with exception " + e.getMessage(),
                        e);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void unregisterAddresses(Collection<InetSocketAddress> addrs) throws IgniteSpiException {
        assert !F.isEmpty(addrs);

        init();

        for (InetSocketAddress addr : addrs) {
            String key = keyFromAddr(addr);
            if (StringUtils.isNotBlank(preferredNetworks) && !key.startsWith(preferredNetworks)) {
                continue;
            }

            try {
                namingService.deregisterInstance(igniteServiceName,
                        addr.getAddress().getHostAddress(), addr.getPort());
            } catch (Exception e) {
                // https://github.com/Azure/azure-sdk-for-java/issues/20551
                if ((!(e.getMessage().contains("InterruptedException"))) || (
                        e instanceof NacosException
                                && (((NacosException) e).getErrCode()
                                != NacosException.NOT_FOUND))) {
                    throw new IgniteSpiException("Failed to delete entry [ entry=" + key + ']', e);
                }
            }
        }
    }

    /**
     * Sets Azure Blob Storage Account Name.
     * <p>
     * For details refer to Azure Blob Storage API reference.
     *
     * @param igniteServiceName Account Name
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = false)
    public TcpDiscoveryNacosIpFinder setIgniteServiceName(String igniteServiceName) {
        this.igniteServiceName = igniteServiceName;

        return this;
    }

    @IgniteSpiConfiguration(optional = false)
    public TcpDiscoveryNacosIpFinder setServerAddr(String serverAddr) {
        this.serverAddr = serverAddr;

        return this;
    }

    /**
     * Sets Azure Blob Storage Account Key
     * <p>
     * For details refer to Azure Blob Storage API reference.
     *
     * @param namespace Account Key
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = false)
    public TcpDiscoveryNacosIpFinder setNamespace(String namespace) {
        this.namespace = namespace;

        return this;
    }

    /**
     * Sets Azure Blob Storage Account Key
     * <p>
     * For details refer to Azure Blob Storage API reference.
     *
     * @param dataId Account Key
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = true)
    public TcpDiscoveryNacosIpFinder setDataId(String dataId) {
        this.dataId = dataId;

        return this;
    }

    @IgniteSpiConfiguration(optional = true)
    public TcpDiscoveryNacosIpFinder setPreferredNetworks(String preferredNetworks) {
        this.preferredNetworks = preferredNetworks;

        return this;
    }

    /**
     * Sets Azure Blob Storage endpoint
     * <p>
     * For details refer to Azure Blob Storage API reference.
     *
     * @param group Endpoint for storage
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = true)
    public TcpDiscoveryNacosIpFinder setGroup(String group) {
        this.group = group;

        return this;
    }

    /**
     * Initialize the IP finder
     */
    private void init() throws IgniteSpiException {
        if (initGuard.compareAndSet(false, true)) {
            if (igniteServiceName == null || serverAddr == null || namespace == null) {
                throw new IgniteSpiException(
                        "One or more of the required parameters is not set ["
                                + "igniteServiceName=" + igniteServiceName +
                                ", serverAddr=" + serverAddr +
                                ", namespace=" + namespace +
                                ", dataId=" + dataId +
                                ", group=" + group + "]");
            }

            try {
                Properties properties = new Properties();
                properties.put("serverAddr", serverAddr);
                properties.put("namespace", namespace);
                namingService = NamingFactory.createNamingService(properties);
            } catch (NacosException e) {
                throw new IgniteSpiException(
                        "Failed to connect nacos serverAddr =" + serverAddr, e);
            } finally {
                initLatch.countDown();
            }
        } else {
            try {
                U.await(initLatch);
            } catch (IgniteInterruptedCheckedException e) {
                throw new IgniteSpiException("Thread has been interrupted.", e);
            }

            try {
                if (namingService == null) {
                    throw new IgniteSpiException("IpFinder has not been initialized properly");
                }
            } catch (Exception e) {
                // Check if this is a nested exception wrapping an InterruptedException
                // https://github.com/Azure/azure-sdk-for-java/issues/20551
                if (!(e.getCause() instanceof InterruptedException)) {
                    throw e;
                }
            }
        }
    }

    /**
     * Constructs bucket's key from an address.
     *
     * @param addr Node address.
     * @return Bucket key.
     */
    private String keyFromAddr(InetSocketAddress addr) {
        // TODO: This needs to move out to a generic helper class
        return addr.getAddress().getHostAddress() + ":" + addr.getPort();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TcpDiscoveryNacosIpFinder setShared(boolean shared) {
        super.setShared(shared);

        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return S.toString(TcpDiscoveryNacosIpFinder.class, this);
    }
}
