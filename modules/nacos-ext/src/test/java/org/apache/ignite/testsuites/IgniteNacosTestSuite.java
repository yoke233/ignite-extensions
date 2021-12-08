package org.apache.ignite.testsuites;
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

import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.spi.discovery.tcp.ipfinder.nacos.TcpDiscoveryNacosIpFinderSelfTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Azure integration tests
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({TcpDiscoveryNacosIpFinderSelfTest.class})
public class IgniteNacosTestSuite {
    /**
     * @return Account Name
     * -Dtest.nacos.serverAddr=nacos.starringshop.com -Dtest.nacos.namespace=59fa2eb3-cbe2-402f-b9c6-234b321788c3 -Dtest.nacos.igniteServiceName=devstoreaccount
     */
    public static String getServerAddr() {
        String id = X.getSystemOrEnv("test.nacos.serverAddr");

        assert id != null : "Environment variable 'test.nacos.serverAddr' is not set";

        return id;
    }

    /**
     * @return Account Key
     */
    public static String getNamespace() {
        String path = X.getSystemOrEnv("test.nacos.namespace");

        assert path != null : "Environment variable 'test.nacos.namespace' is not set";

        return path;
    }

    /**
     * @return Endpoint
     */
    public static String getIgniteServiceName() {
        String name = X.getSystemOrEnv("test.nacos.igniteServiceName");

        assert name != null : "Environment variable 'test.nacos.igniteServiceName' is not set";

        return name;
    }
}
