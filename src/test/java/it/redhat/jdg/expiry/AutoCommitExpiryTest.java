package it.redhat.jdg.expiry;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryExpired;
import org.infinispan.notifications.cachelistener.event.CacheEntryExpiredEvent;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.junit.*;

import javax.transaction.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class AutoCommitExpiryTest {

    private EmbeddedCacheManager cacheManager;
    private Cache<String, String> applicationCache;

    @Test
    public void testNoAutCommitAndExpiryListener() {
        ExpiryListener expiryListener = new ExpiryListener(2);

        cacheManager = startCacheManager(false);
        applicationCache = cacheManager.getCache("noautocommit", true);
        applicationCache.addListener(expiryListener);

        TransactionManager tm = applicationCache.getAdvancedCache().getTransactionManager();
        try {
            tm.begin();
            applicationCache.put("test1", "value1", 1, TimeUnit.SECONDS);
            tm.commit();

            tm.begin();
            applicationCache.put("test2", "value2", 1, TimeUnit.SECONDS);
            tm.commit();
        } catch (RollbackException | HeuristicMixedException | SystemException | HeuristicRollbackException | NotSupportedException e) {
            Assert.fail(e.getMessage());
        }

        try {
            boolean result = expiryListener.await(5);
            Assert.assertTrue("Entries are not expired properly", result);
        } catch (InterruptedException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Listener(primaryOnly = true, observation = Listener.Observation.POST)
    public class ExpiryListener {

        private final CountDownLatch counter;

        public ExpiryListener(int count) {
            counter = new CountDownLatch(count);
        }

        @CacheEntryExpired
        public void expired(CacheEntryExpiredEvent<String, String> event) {
            if (counter.getCount() == 0) {
                Assert.fail("Counter already zero");
            }
            counter.countDown();
        }

        public boolean await(int timeout) throws InterruptedException {
            return counter.await(timeout, TimeUnit.SECONDS);
        }

    }

    private EmbeddedCacheManager startCacheManager(boolean autocommit) {
        GlobalConfiguration glob = new GlobalConfigurationBuilder().clusteredDefault()
                .transport().addProperty("configurationFile", System.getProperty("jgroups.configuration", "jgroups-udp.xml"))
                .globalJmxStatistics().allowDuplicateDomains(true).enable()
                .build();

        Configuration distributedTransactionalConf = new ConfigurationBuilder()
                .jmxStatistics().enable()
                .clustering()
                .cacheMode(CacheMode.DIST_SYNC)
                .hash().numOwners(2)
                .transaction()
                .transactionMode(TransactionMode.TRANSACTIONAL)
                .transactionManagerLookup(new DummyTransactionManagerLookup())
                .autoCommit(autocommit)
                .expiration()
                .enableReaper().wakeUpInterval(200, TimeUnit.MILLISECONDS)
                .lifespan(20, TimeUnit.SECONDS)
                .build();

        return new DefaultCacheManager(glob, distributedTransactionalConf, true);
    }
}
