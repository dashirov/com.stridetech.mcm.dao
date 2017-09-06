package com.stridetech.mcm.dao;



import com.stridetech.mcm.config.ApplicationConfiguration;
import com.stridetech.mcm.model.enums.*;
import com.stridetech.mcm.model.logs.ChangeLog;
import com.stridetech.mcm.model.logs.ChangeLogEntry;
import com.stridetech.mcm.model.meta.*;
import com.stridetech.mcm.util.AccountComparator;
import com.stridetech.mcm.util.CampaignComparator;
import com.stridetech.mcm.util.MarketplaceComparator;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.function.Consumer;

@RunWith(SpringJUnit4ClassRunner.class)
@Import(ApplicationConfiguration.class)
public class MCMServiceDaoTest {

    @Autowired
    private MCMServiceDao mcmServiceDao;

    /**
     * BUSINESS UNIT ACCOUNT
     **/

    @Test
    @Transactional
    @Rollback(true)
    public void testCreateBusinessUnit() {
        Account a = new Account();
        a.setStatus(AccountStatus.ACTIVE);
        a.setStatusUpdated(new Date());
        a.setType(AccountType.BUSINESS_UNIT);
        a.setName("Test Business Unit");
        a.setDescription("No description");

        Account created = mcmServiceDao.createBusinessUnit(a);
        Assert.assertNotNull(created.getId());
    }

    @Test
    @Transactional
    @Rollback(true)
    public void testRetrieveBusinessUnit() {
        // place some values into DTO
        Account a = new Account();
        a.setStatus(AccountStatus.ACTIVE);
        a.setStatusUpdated(new Date());
        a.setType(AccountType.BUSINESS_UNIT);
        a.setName("Test Business Unit");
        a.setDescription("No description");

        // Create an account and pull autogenerated Id
        Account created = mcmServiceDao.createBusinessUnit(a);

        // Go back to the backend to re-retrieve another copy of the record and cross reference the two
        Account retrieved = mcmServiceDao.retrieveBusinessUnit(created.getId());
        Assert.assertEquals(created.getId(), retrieved.getId());
        Assert.assertEquals(created.getName(), retrieved.getName());
        Assert.assertEquals(created.getDescription(), retrieved.getDescription());
        Assert.assertEquals(created.getStatusUpdated().getTime(), retrieved.getStatusUpdated().getTime());
        Assert.assertEquals(created.getStatus(), retrieved.getStatus());
    }

    @Test
    @Transactional
    @Rollback(true)
    public void testUpdateBusinessUnit() {
        Account a = new Account();
        a.setType(AccountType.BUSINESS_UNIT);
        a.setStatus(AccountStatus.ACTIVE);
        a.setStatusUpdated(new Date());
        a.setName("Another Test business unit");
        a.setDescription("No Description");


        a = mcmServiceDao.createBusinessUnit(a);
        a.setName("B2B Business Unit");

        // Don't forget to change the effective date on status change! That is your responsibility.
        a.setStatus(AccountStatus.PAUSED);
        a.setStatusUpdated(new Date());

        a = mcmServiceDao.updateBusinessUnit(a);
        Assert.assertNotNull(a);
        Assert.assertEquals("B2B Business Unit", a.getName());

        ChangeLog changeLog = mcmServiceDao.getAccountStatusChangelog(a);
        Assert.assertEquals(2, changeLog.getChangeLogs().size());
        //TODO: Add more tests here.
    }

    @Test
    @Transactional
    @Rollback(true)
    public void testDeleteBusinessUnit() throws ParseException {
        Random rnd = new Random();

        DateFormat f = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        Account a = new Account();
        a.setType(AccountType.BUSINESS_UNIT);
        a.setName("Test account Random#" + rnd.nextInt());

        a.setStatus(AccountStatus.ACTIVE);
        a.setStatusUpdated(f.parse("2016-01-01 00:00:00.001"));

        a = mcmServiceDao.createBusinessUnit(a);

        a.setStatus(AccountStatus.PAUSED);
        a.setStatusUpdated(f.parse("2016-06-01 00:00:00.001"));

        a = mcmServiceDao.updateBusinessUnit(a);

        Account b = mcmServiceDao.deleteBusinessUnit(a);

        Assert.assertNotNull(b);
        Assert.assertNotNull(b.getId());

    }

    @Test
    @Transactional
    @Rollback(true)
    public void testListCurrentBusinessUnits() {
        SortedSet<Account> createdBusinessUnits = new TreeSet<>(new AccountComparator());
        for (int i = 20; i < 30; i++) {
            Account a = new Account();
            a.setType(AccountType.BUSINESS_UNIT);
            Random rnd = new Random();
            a.setStatus(AccountStatus.values()[rnd.nextInt(AccountStatus.values().length)]);
            a.setStatusUpdated(new Date());
            a.setName("Business Unit #" + i + " status: " + a.getStatus().name());
            a = mcmServiceDao.createBusinessUnit(a);
            createdBusinessUnits.add(a);
        }
        SortedSet<Account> retrievedBusinessUnits = mcmServiceDao.listBusinessUnits();
        for (Account a : createdBusinessUnits
                ) {
            switch (a.getStatus()) {
                case ACTIVE:
                    Assert.assertTrue(retrievedBusinessUnits.contains(a));
                    break;
                case PAUSED:
                    Assert.assertTrue(retrievedBusinessUnits.contains(a));
                    break;
                case DELETED:
                    Assert.assertFalse(retrievedBusinessUnits.contains(a));
                    break;
                default:
                    break;
            }

        }
    }

    @Test
    @Transactional(isolation = Isolation.READ_COMMITTED)
    @Rollback(true)
    public void testListBusinessUnitsInRetrospect() throws ParseException {

        final SortedSet<Account> createdBusinessUnits = new TreeSet<>(new AccountComparator());
        final SortedSet<Account> retrievedBusinessUnits = mcmServiceDao.listBusinessUnits();

        final Random rnd = new Random();

        final DateFormat f = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

        final Date dateActive = f.parse("2016-01-01 00:00:00.000");
        final Date datePAUSED = f.parse("2016-06-01 00:00:00.000");
        final Date dateDeleted = f.parse("2016-12-31 23:59:59.999");

        /**
         * 1. Create 10 business units on dateActive date
         * 2. Suspend first five of the business units on datePAUSED date
         * 3. Delete first 2 business units on dateDelete date
         * 4. check in retrospect before dateActive ( should be 0 accounts )
         * 5.                     after dateActive and before dateSuspend (10 accounts, all active )
         * 6.                     after dateSuspend and before dateDelete (10 accounts, 5 active, 5 PAUSED )
         * 7                      after dateDelete (8 accounts, 5 active, 3 PAUSED )
         */


        for (int i = 0; i < 10; i++) {
            Account a = new Account();
            a.setType(AccountType.BUSINESS_UNIT);

            a.setStatus(AccountStatus.ACTIVE);
            a.setStatusUpdated(dateActive);

            a.setName("Business Unit #" + i + " status: " + a.getStatus().name());
            a.setDescription("ACTVIVE: " + dateActive.toString());

            a = mcmServiceDao.createBusinessUnit(a);

            if (i < 5) {
                a.setStatus(AccountStatus.PAUSED);
                a.setStatusUpdated(datePAUSED);
                a = mcmServiceDao.updateBusinessUnit(a);
            }

            if (i < 2) {
                a.setStatus(AccountStatus.DELETED);
                a.setStatusUpdated(dateDeleted);
                a = mcmServiceDao.updateBusinessUnit(a);
            }
            createdBusinessUnits.add(a);
            System.out.println(mcmServiceDao.getAccountStatusChangelog(a).toString());
        }


        SortedSet<Account> vacuum = mcmServiceDao.listBusinessUnitsInRetrospect(Date.from(dateActive.toInstant().minus(1, ChronoUnit.SECONDS)));
        Assert.assertEquals(0, vacuum.size());

        SortedSet<Account> all_active = mcmServiceDao.listBusinessUnitsInRetrospect(Date.from(dateActive.toInstant().plus(1, ChronoUnit.SECONDS)));
        Assert.assertEquals(10, all_active.size());

        SortedSet<Account> some_active_some_PAUSED = mcmServiceDao.listBusinessUnitsInRetrospect(datePAUSED);
        Assert.assertEquals(10, some_active_some_PAUSED.size());
        Assert.assertEquals(5, some_active_some_PAUSED.stream().filter(t -> t.getStatus().equals(AccountStatus.ACTIVE)).count());
        Assert.assertEquals(5, some_active_some_PAUSED.stream().filter(t -> t.getStatus().equals(AccountStatus.PAUSED)).count());

        SortedSet<Account> less_deleted = mcmServiceDao.listBusinessUnitsInRetrospect(dateDeleted);
        Assert.assertEquals(8, less_deleted.size());
        Assert.assertEquals(5, less_deleted.stream().filter(t -> t.getStatus().equals(AccountStatus.ACTIVE)).count());
        Assert.assertEquals(3, less_deleted.stream().filter(t -> t.getStatus().equals(AccountStatus.PAUSED)).count());

    }

    /**
     * MARKETPLACE
     **/

    @Test
    @Transactional
    @Rollback(true)
    public void testCreateMarketplace() {
        Marketplace m = new Marketplace();
        m.setName("Facebook");
        m.setDescription("Facebook and Instagram marketing spend");
        m.setStatusUpdated(new Date());
        m.setStatus(MarketplaceStatus.ACTIVE);

        Marketplace out = mcmServiceDao.createMarketplace(m);
        Assert.assertNotNull(out);
        Assert.assertNotNull(out.getId());

        Assert.assertEquals(m.getName(), out.getName());
        Assert.assertEquals(m.getDescription(), out.getDescription());
        Assert.assertEquals(m.getStatus(), out.getStatus());
        Assert.assertEquals(m.getStatusUpdated(), out.getStatusUpdated());

    }

    @Test
    @Transactional
    @Rollback(true)
    public void testRetrieveMarketplace() {
        Marketplace m = new Marketplace();
        m.setName("Facebook");
        m.setDescription("Facebook and Instagram marketing spend");
        m.setStatusUpdated(new Date());
        m.setStatus(MarketplaceStatus.ACTIVE);

        Marketplace out = mcmServiceDao.createMarketplace(m);

        Marketplace retrieved = mcmServiceDao.retrieveMarketplace(out.getId());
        Assert.assertNotNull(retrieved);
        Assert.assertEquals(out.getId(), retrieved.getId());
        Assert.assertEquals(out.getName(), retrieved.getName());
        Assert.assertEquals(out.getDescription(), retrieved.getDescription());
        Assert.assertEquals(out.getStatus(), retrieved.getStatus());
        Assert.assertEquals(out.getStatusUpdated(), retrieved.getStatusUpdated());
    }

    @Test
    @Transactional
    @Rollback(true)
    public void testUpdateMarketplace() {
        Marketplace m = new Marketplace();
        m.setName("Facebook");
        m.setDescription("Facebook and Instagram marketing spend");
        m.setStatusUpdated(new Date());
        m.setStatus(MarketplaceStatus.ACTIVE);

        Marketplace out = mcmServiceDao.createMarketplace(m);
        Assert.assertNotNull(out);
        out.setName(m.getName() + " UPDATED");
        out.setDescription(m.getDescription() + "UPDATED");
        out.setStatus(MarketplaceStatus.TERMINATED);
        out.setStatusUpdated(new Date());

        Marketplace updated = mcmServiceDao.updateMarketplace(out);
        Assert.assertNotNull(updated);

        Marketplace retrieved = mcmServiceDao.retrieveMarketplace(out.getId());
        Assert.assertNotNull(retrieved);

        Assert.assertEquals(out.getId(), retrieved.getId());
        Assert.assertEquals(out.getName(), retrieved.getName());
        Assert.assertEquals(out.getDescription(), retrieved.getDescription());
        Assert.assertEquals(out.getStatus(), retrieved.getStatus());
        Assert.assertEquals(out.getStatusUpdated(), retrieved.getStatusUpdated());


    }

    @Test
    @Transactional
    @Rollback(true)
    public void testDeleteMarketplace() throws ParseException {
        Random rnd = new Random();
        DateFormat f = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

        Marketplace m = new Marketplace();
        m.setName("Random marketplace #" + rnd.nextInt());
        m.setStatus(MarketplaceStatus.ACTIVE);
        m.setStatusUpdated(f.parse("2016-01-01 00:00:00.001"));

        m = mcmServiceDao.createMarketplace(m);

        Marketplace d = mcmServiceDao.deleteMarketplace(m);

        Assert.assertNotNull(d.getId());
        Assert.assertEquals(MarketplaceStatus.DELETED, d.getStatus());
        Assert.assertTrue(d.getStatusUpdated().after(f.parse("2016-01-01 00:00:00.001")));
    }

    @Test
    @Transactional(isolation = Isolation.REPEATABLE_READ)
    @Rollback(true)
    public void testListCurrentMarketplaces() {
        SortedSet<Marketplace> createdMarketplaces = new TreeSet<>(new MarketplaceComparator());
        Random rnd = new Random();
        for (int i = 0; i < 10; i++) {
            Marketplace m = new Marketplace();
            m.setName("Marketplace #" + rnd.nextInt());
            m.setStatus(MarketplaceStatus.values()[rnd.nextInt(MarketplaceStatus.values().length)]);
            m.setStatusUpdated(new Date());
            m = mcmServiceDao.createMarketplace(m);
            createdMarketplaces.add(m);

            SortedSet<Marketplace> retrievedMarketplaces = mcmServiceDao.listMarketplaces();
            for (Marketplace mk : createdMarketplaces
                    ) {
                switch (mk.getStatus()) {
                    case ACTIVE:
                        Assert.assertTrue(retrievedMarketplaces.contains(mk));
                        break;
                    case PAUSED:
                        Assert.assertTrue(retrievedMarketplaces.contains(mk));
                        break;
                    case MERGED:
                        Assert.assertTrue(retrievedMarketplaces.contains(mk));
                        break;
                    case TERMINATED:
                        Assert.assertTrue(retrievedMarketplaces.contains(mk));
                        break;
                    case DELETED:
                        Assert.assertFalse(retrievedMarketplaces.contains(mk));
                        break;
                    default:
                        break;
                }
            }

        }
    }

    @Test
    @Transactional
    @Rollback(true)
    public void testListMarketplacesInRetrospect() throws ParseException {
        final MarketplaceComparator mc = new MarketplaceComparator();
        final SortedSet<Marketplace> createdMarketplaces = new TreeSet<>(mc);
        final SortedSet<Marketplace> retrievedMarketplaces = new TreeSet<>(mc);
        final DateFormat f = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        final Date dateActive = f.parse("2016-01-01 00:00:00.000");
        final Date datePaused = f.parse("2016-06-01 00:00:00.000");
        final Date dateDeleted = f.parse("2016-12-31 23:59:59.999");
        final Random rnd = new Random();

        for (int i = 0; i < 10; i++) {
            Marketplace m = new Marketplace();
            m.setName("Random marketplace #" + rnd.nextInt());
            m.setStatus(MarketplaceStatus.ACTIVE);
            m.setStatusUpdated(dateActive);
            m = mcmServiceDao.createMarketplace(m);

            if (i < 5) {
                m.setStatus(MarketplaceStatus.PAUSED);
                m.setStatusUpdated(datePaused);
                mcmServiceDao.updateMarketplace(m);
            }

            if (i < 2) {
//                m = mcmServiceDao.deleteMarketplace(m); // this delete occurs NOW, not on date deleted.
                m.setStatus(MarketplaceStatus.DELETED);
                m.setStatusUpdated(dateDeleted);
                mcmServiceDao.updateMarketplace(m);
            }
            createdMarketplaces.add(m);
            System.out.println(m.getName());
            mcmServiceDao.getMarketplaceStatusChangelog(m).getChangeLogs().stream().forEachOrdered(new Consumer<ChangeLogEntry<MarketplaceStatus>>() {
                @Override
                public void accept(ChangeLogEntry<MarketplaceStatus> marketplaceStatusChangeLogEntry) {
                    System.out.println(marketplaceStatusChangeLogEntry.effectiveDate + " " + marketplaceStatusChangeLogEntry.value.name());
                }
            });
        }
        SortedSet<Marketplace> vacuum = mcmServiceDao.listMarketplacesInRetrospect(Date.from(dateActive.toInstant().minus(1, ChronoUnit.SECONDS)));
        Assert.assertNotNull(vacuum);
        Assert.assertEquals(0, vacuum.size());


        SortedSet<Marketplace> universe = mcmServiceDao.listMarketplacesInRetrospect(Date.from(dateActive.toInstant().plus(1, ChronoUnit.SECONDS)));
        Assert.assertNotNull(universe);
        Assert.assertEquals(10, universe.size());
        Assert.assertTrue(universe.stream().allMatch(t -> t.getStatus().equals(MarketplaceStatus.ACTIVE)));

        SortedSet<Marketplace> some_active_some_paused = mcmServiceDao.listMarketplacesInRetrospect(datePaused);
        Assert.assertNotNull(some_active_some_paused);
        Assert.assertEquals(10, some_active_some_paused.size());
        Assert.assertEquals(5, some_active_some_paused.stream().filter(t -> t.getStatus().equals(MarketplaceStatus.ACTIVE)).count());
        Assert.assertEquals(5, some_active_some_paused.stream().filter(t -> t.getStatus().equals(MarketplaceStatus.PAUSED)).count());
        SortedSet<Marketplace> less_deleted = mcmServiceDao.listMarketplacesInRetrospect(dateDeleted);
        Assert.assertNotNull(less_deleted);
        Assert.assertEquals(8, less_deleted.size());
        Assert.assertEquals(5, less_deleted.stream().filter(t -> t.getStatus().equals(MarketplaceStatus.ACTIVE)).count());
        Assert.assertEquals(3, less_deleted.stream().filter(t -> t.getStatus().equals(MarketplaceStatus.PAUSED)).count());

    }

    // Product //

    @Test
    @Transactional
    @Rollback(true)
    public void testCreateProduct() {
        Date creationDate = new Date();
        Account a = new Account();
        a.setStatus(AccountStatus.ACTIVE);
        a.setStatusUpdated(creationDate);
        a.setName("Business Unit");
        mcmServiceDao.createBusinessUnit(a);

        Product p = new Product();

        p.setId("WDI");
        p.setName("Web Development Immersive");
        p.setDescription("START YOUR CAREER IN WEB DEVELOPMENT");
        p.setStatus(ProductStatus.ACTIVE);
        p.setStatusUpdated(creationDate);


        Product created = mcmServiceDao.createProduct(p);
        Assert.assertNotNull(created);
        Assert.assertEquals("WDI", created.getId());
        Assert.assertEquals("Web Development Immersive", created.getName());
        Assert.assertEquals("START YOUR CAREER IN WEB DEVELOPMENT", created.getDescription());
        Assert.assertEquals(ProductStatus.ACTIVE, created.getStatus());
        Assert.assertEquals(creationDate, created.getStatusUpdated());

        Assert.assertEquals(1, mcmServiceDao.getProductStatusChangelog(created).getChangeLogs().size());


    }

    @Test
    @Transactional
    @Rollback(true)
    public void testRetrieveProduct() {
        Date creationDate = new Date();

        Account a = new Account();
        a.setStatus(AccountStatus.ACTIVE);
        a.setStatusUpdated(creationDate);
        a.setName("Business Unit");
        mcmServiceDao.createBusinessUnit(a);

        Product p = new Product();
        p.setId("WDI");
        p.setName("Web Development Immersive");
        p.setDescription("START YOUR CAREER IN WEB DEVELOPMENT");
        p.setStatus(ProductStatus.ACTIVE);
        p.setStatusUpdated(creationDate);
        Product created = mcmServiceDao.createProduct(p);

        Product retrieved = mcmServiceDao.retrieveProduct(p.getId());
        Assert.assertNotNull(retrieved);

        Assert.assertEquals("WDI", retrieved.getId());
        Assert.assertEquals("Web Development Immersive", retrieved.getName());
        Assert.assertEquals("START YOUR CAREER IN WEB DEVELOPMENT", retrieved.getDescription());
        Assert.assertEquals(ProductStatus.ACTIVE, retrieved.getStatus());
        Assert.assertEquals(creationDate, retrieved.getStatusUpdated());

    }

    @Test
    @Transactional
    @Rollback(true)
    public void testUpdateProduct() {
        Date creationDate = new Date();
        Account a = new Account();
        a.setStatus(AccountStatus.ACTIVE);
        a.setStatusUpdated(creationDate);
        a.setName("Business Unit");
        mcmServiceDao.createBusinessUnit(a);

        Product p = new Product();
        p.setId("WDIR");
        p.setName("Web Development Immersive - Remote");
        p.setDescription("START YOUR CAREER IN WEB DEVELOPMENT");
        p.setStatus(ProductStatus.ACTIVE);
        p.setStatusUpdated(creationDate);
        p = mcmServiceDao.createProduct(p);
        p.setStatus(ProductStatus.PAUSED);
        p.setStatusUpdated(new Date());
        p.setName(p.getName() + " UPDATED");
        p.setDescription(p.getDescription() + " UPDATED");
        Product updated = mcmServiceDao.updateProduct(p);
        ChangeLog<ProductStatus> log = mcmServiceDao.getProductStatusChangelog(p);
        Assert.assertEquals(2, log.getChangeLogs().size());
        Assert.assertEquals(ProductStatus.ACTIVE, log.getChangeLogs().last().value);
        Assert.assertEquals(ProductStatus.PAUSED, log.getChangeLogs().first().value);

    }

    @Test
    @Transactional
    @Rollback(true)
    public void testDeleteProduct() {
        Date creationDate = new Date();
        Account a = new Account();
        a.setStatus(AccountStatus.ACTIVE);
        a.setStatusUpdated(creationDate);
        a.setName("Business Unit");
        mcmServiceDao.createBusinessUnit(a);

        Product p = new Product();

        p.setId("WDIR");
        p.setName("Web Development Immersive - Remote");
        p.setDescription("START YOUR CAREER IN WEB DEVELOPMENT");
        p.setStatus(ProductStatus.ACTIVE);
        p.setStatusUpdated(creationDate);
        p = mcmServiceDao.createProduct(p);
        p.setStatus(ProductStatus.PAUSED); // ignored by the delete
        p.setStatusUpdated(new Date()); // ignored by the delete
        p.setName(p.getName() + " UPDATED");  // should be ignored by the delete
        p.setDescription(p.getDescription() + " UPDATED"); // should be ignored by the delete
        Product deleted = mcmServiceDao.deleteProduct(p);
        Assert.assertEquals(ProductStatus.DELETED, deleted.getStatus());
        ChangeLog<ProductStatus> log = mcmServiceDao.getProductStatusChangelog(deleted);
        Assert.assertEquals(2, log.getChangeLogs().size());
        Assert.assertEquals(ProductStatus.ACTIVE, log.getChangeLogs().last().value);
        Assert.assertEquals(ProductStatus.DELETED, log.getChangeLogs().first().value);
    }

    // Campaign //

    @Test
    @Transactional
    @Rollback(true)
    public void testCreateCampaign(){
        Account a = new Account();
        a.setStatus(AccountStatus.ACTIVE);
        a.setStatusUpdated(new Date());
        a.setName("Business Unit");
        mcmServiceDao.createBusinessUnit(a);

        Product p = new Product();

        p.setStatus(ProductStatus.ACTIVE);
        p.setStatusUpdated(new Date());
        p.setId("WDI");
        p.setName("Web DEvelopment Immersive OnPrem");
        p= mcmServiceDao.createProduct(p);

        Marketplace m = new Marketplace();
        m.setName("Facebook");
        m.setStatus(MarketplaceStatus.ACTIVE);
        m.setStatusUpdated(new Date());
        m= mcmServiceDao.createMarketplace(m);

        Campaign c = new Campaign();
        c.setProduct(p.getId());
        c.setMarketplace(m.getId());
        c.setTracker(p.getId()+"^drm001");
        c.setName(p.getId() + " " + m.getName() +" SEM direct response marketing in US/NYC Metro");
        c.setStatus(CampaignStatus.ACTIVE);
        c.setStatusUpdated(new Date());
        c.setType(CampaignType.CPC);
        c.setBusinessUnit(a.getId());
        c = mcmServiceDao.createCampaign(c);

        Assert.assertNotNull(c);

    }
    @Test
    @Transactional
    @Rollback(true)
    public void testRetrieveCampaign(){
        Account a = new Account();
        a.setStatus(AccountStatus.ACTIVE);
        a.setStatusUpdated(new Date());
        a.setName("Business Unit");
        mcmServiceDao.createBusinessUnit(a);

        Product p = new Product();

        p.setStatus(ProductStatus.ACTIVE);
        p.setStatusUpdated(new Date());
        p.setId("WDI");
        p.setName("Web DEvelopment Immersive OnPrem");
        p= mcmServiceDao.createProduct(p);

        Marketplace m = new Marketplace();
        m.setName("Facebook");
        m.setStatus(MarketplaceStatus.ACTIVE);
        m.setStatusUpdated(new Date());
        m= mcmServiceDao.createMarketplace(m);

        Campaign c = new Campaign();
        c.setBusinessUnit(a.getId());
        c.setProduct(p.getId());
        c.setMarketplace(m.getId());
        c.setTracker(p.getId()+"^drm001");
        c.setName(p.getId() + " " + m.getName() +" SEM direct response marketing in US/NYC Metro");
        c.setStatus(CampaignStatus.ACTIVE);
        c.setStatusUpdated(new Date());
        c.setType(CampaignType.CPC);
        c = mcmServiceDao.createCampaign(c);

        Campaign retrieved = mcmServiceDao.retrieveCampaign(c.getTracker());
        Assert.assertNotNull(retrieved);
    }

    @Test
    @Transactional
    @Rollback(true)
    public void testUpdateCampaign(){
        Account a = new Account();
        a.setStatus(AccountStatus.ACTIVE);
        a.setStatusUpdated(new Date());
        a.setName("Business Unit");
        mcmServiceDao.createBusinessUnit(a);

        Product p = new Product();
        p.setStatus(ProductStatus.ACTIVE);
        p.setStatusUpdated(new Date());
        p.setId("WDI");
        p.setName("Web DEvelopment Immersive OnPrem");
        p= mcmServiceDao.createProduct(p);

        Marketplace m = new Marketplace();
        m.setName("Facebook");
        m.setStatus(MarketplaceStatus.ACTIVE);
        m.setStatusUpdated(new Date());
        m= mcmServiceDao.createMarketplace(m);

        Marketplace m1 = new Marketplace();
        m1.setName("Google");
        m1.setStatus(MarketplaceStatus.ACTIVE);
        m1.setStatusUpdated(new Date());
        m1 = mcmServiceDao.createMarketplace(m1);

        Campaign c = new Campaign();
        c.setProduct(p.getId());
        c.setBusinessUnit(a.getId());
        c.setMarketplace(m.getId());
        c.setTracker(p.getId()+"^drm001");
        c.setName(p.getId() + " " + m.getName() +" SEM direct response marketing in US/NYC Metro");
        c.setStatus(CampaignStatus.ACTIVE);
        c.setStatusUpdated(new Date());
        c.setType(CampaignType.CPC);
        Campaign created = mcmServiceDao.createCampaign(c);

        created.setName("Another name");
        created.setDescription("Another description");
        created.setBusinessUnit(42L);
        created.setStatus(CampaignStatus.PAUSED);
        created.setStatusUpdated(new Date());
        created.setMarketplace(m1.getId());

        Campaign updated = mcmServiceDao.updateCampaign(created);
        Assert.assertNotNull(updated);

        Campaign retrieved = mcmServiceDao.retrieveCampaign(created.getTracker());
        Assert.assertEquals(m1.getId(), retrieved.getMarketplace());
    }

    @Test
    @Transactional
    @Rollback(true)
    public void testDeleteCampaign(){
        Account a = new Account();
        a.setStatus(AccountStatus.ACTIVE);
        a.setStatusUpdated(new Date());
        a.setName("Business Unit");
        mcmServiceDao.createBusinessUnit(a);

        Product p = new Product();
        p.setStatus(ProductStatus.ACTIVE);
        p.setStatusUpdated(new Date());
        p.setId("WDI");
        p.setName("Web Development Immersive OnPrem");
        p= mcmServiceDao.createProduct(p);

        Marketplace m = new Marketplace();
        m.setName("Facebook");
        m.setStatus(MarketplaceStatus.ACTIVE);
        m.setStatusUpdated(new Date());
        m= mcmServiceDao.createMarketplace(m);

        Campaign c = new Campaign();
        c.setBusinessUnit(a.getId());
        c.setProduct(p.getId());
        c.setMarketplace(m.getId());
        c.setTracker(p.getId()+"^drm001");
        c.setName(p.getId() + " " + m.getName() +" SEM direct response marketing in US/NYC Metro");
        c.setStatus(CampaignStatus.ACTIVE);
        c.setStatusUpdated(new Date());
        c.setType(CampaignType.CPC);
        c = mcmServiceDao.createCampaign(c);

        Assert.assertNotNull(c);

        Campaign retrieved = mcmServiceDao.retrieveCampaign(c.getTracker());
        Campaign deleted = mcmServiceDao.deleteCampaign(retrieved );

        Assert.assertNotNull(deleted);
        Assert.assertEquals(CampaignStatus.DELETED, deleted.getStatus());
        SortedSet<Campaign> campaigns = mcmServiceDao.listCampaigns();
        Assert.assertFalse(campaigns.contains(deleted));
        ChangeLog<CampaignStatus> campaignStatusChangeLog = mcmServiceDao.getCampaignStatusChangelog(deleted);
        Assert.assertEquals(2, campaignStatusChangeLog.getChangeLogs().size());
        Assert.assertEquals(CampaignStatus.DELETED, campaignStatusChangeLog.getValue(new Date(), null));
        Assert.assertNull(campaignStatusChangeLog.getValue(Date.from(new Date().toInstant().minus(1, ChronoUnit.DAYS)), null));
    }

    @Test
    @Transactional
    @Rollback(true)
    public void testListCampaignsInRetrospect() throws ParseException{
        final SortedSet<Campaign> createdCampaigns = new TreeSet<>(new CampaignComparator());
        final SortedSet<Campaign> retrievedCampaigns = new TreeSet<>(new CampaignComparator());

        final String Passport= "487961361 06-Mar-2022";

        final Random rnd = new Random();

        final DateFormat f = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

        final Date dateActive = f.parse("2016-01-01 00:00:00.000");
        final Date datePAUSED = f.parse("2016-06-01 00:00:00.000");
        final Date dateDeleted = f.parse("2016-12-31 23:59:59.999");
        Account a = new Account();
        a.setStatus(AccountStatus.ACTIVE);
        a.setStatusUpdated(dateActive);
        a.setName("Business Unit");
        mcmServiceDao.createBusinessUnit(a);

        Product p = new Product();

        p.setStatus(ProductStatus.ACTIVE);
        p.setStatusUpdated(dateActive);
        p.setId("WDI");
        p.setName("Web DEvelopment Immersive OnPrem");
        p= mcmServiceDao.createProduct(p);

        Marketplace m = new Marketplace();
        m.setName("Facebook");
        m.setStatus(MarketplaceStatus.ACTIVE);
        m.setStatusUpdated(dateActive);
        m= mcmServiceDao.createMarketplace(m);

        /**
         * 1. Create 10 campaigns units on dateActive date
         * 2. Suspend first five of them on datePAUSED date
         * 3. Delete first 2  on dateDelete date
         * 4. check in retrospect before dateActive ( should be 0 accounts )
         * 5.                     after dateActive and before dateSuspend (10 accounts, all active )
         * 6.                     after dateSuspend and before dateDelete (10 accounts, 5 active, 5 PAUSED )
         * 7                      after dateDelete (8 accounts, 5 active, 3 PAUSED )
         */


        for (int i = 0; i < 10; i++) {
            Campaign c = new Campaign();
            c.setProduct(p.getId());
            c.setMarketplace(m.getId());
            c.setTracker(p.getId()+"^drm00"+i);
            c.setBusinessUnit(a.getId());
            c.setName(p.getId() + " " + m.getName() +" SEM "+rnd.nextInt()+" US/NYC Metro ");
            c.setStatus(CampaignStatus.ACTIVE);
            c.setStatusUpdated(dateActive);
            c.setType(CampaignType.CPC);
            c = mcmServiceDao.createCampaign(c);


            if (i < 5) {
                c.setStatus(CampaignStatus.PAUSED);
                c.setStatusUpdated(datePAUSED);
                c = mcmServiceDao.updateCampaign(c);
            }

            if (i < 2) {
                c.setStatus(CampaignStatus.DELETED);
                c.setStatusUpdated(dateDeleted);
                c = mcmServiceDao.updateCampaign(c);
            }
            createdCampaigns.add(c);
            System.out.println(mcmServiceDao.getCampaignStatusChangelog(c).toString());
        }


        SortedSet<Campaign> vacuum = mcmServiceDao.listCampaignsInRetrospect(Date.from(dateActive.toInstant().minus(1, ChronoUnit.SECONDS)));
        Assert.assertEquals(0, vacuum.size());

        SortedSet<Campaign> all_active = mcmServiceDao.listCampaignsInRetrospect(Date.from(dateActive.toInstant().plus(1, ChronoUnit.SECONDS)));
        Assert.assertEquals(10, all_active.size());

        SortedSet<Campaign> some_active_some_PAUSED = mcmServiceDao.listCampaignsInRetrospect(datePAUSED);
        Assert.assertEquals(10, some_active_some_PAUSED.size());
        Assert.assertEquals(5, some_active_some_PAUSED.stream().filter(t -> t.getStatus().equals(CampaignStatus.ACTIVE)).count());
        Assert.assertEquals(5, some_active_some_PAUSED.stream().filter(t -> t.getStatus().equals(CampaignStatus.PAUSED)).count());

        SortedSet<Campaign> less_deleted = mcmServiceDao.listCampaignsInRetrospect(dateDeleted);
        Assert.assertEquals(8, less_deleted.size());
        Assert.assertEquals(5, less_deleted.stream().filter(t -> t.getStatus().equals(CampaignStatus.ACTIVE)).count());
        Assert.assertEquals(3, less_deleted.stream().filter(t -> t.getStatus().equals(CampaignStatus.PAUSED)).count());

        p.setStatus(ProductStatus.DELETED);
        p.setStatusUpdated(dateDeleted);
        mcmServiceDao.updateProduct(p);

        SortedSet<Campaign> campaigns = mcmServiceDao.listCampaignsInRetrospect(dateDeleted);
        Assert.assertEquals("Product deleted - all campaigns are hidden",0,campaigns.size());

        p.setStatus(ProductStatus.PAUSED);
        p.setStatusUpdated(dateDeleted);
        mcmServiceDao.updateProduct(p);

        campaigns = mcmServiceDao.listCampaignsInRetrospect(dateDeleted);
        Assert.assertEquals("Product un-deleted - all non-deleted campaigns are un-hidden",8,campaigns.size());

        m.setStatus(MarketplaceStatus.DELETED);
        m.setStatusUpdated(dateDeleted);
        mcmServiceDao.updateMarketplace(m);

        campaigns = mcmServiceDao.listCampaignsInRetrospect(dateDeleted);
        Assert.assertEquals("Product deleted - all campaigns are hidden",0,campaigns.size());

        m.setStatus(MarketplaceStatus.PAUSED);

    }
    @Test
    @Transactional
    @Rollback(true)
    public void testTagging(){

        // TODO: add tests
        // 1. [✓] create account, product, marketplace
        // 2. [✓]create a campaign
        // 3. create a tag group set to
        //     3.1 mutex on
        //     3.2 applicable to CAMPAIGN
        //     3.3 create a few tags under the tag group
        // 4. create a tag group set to
        //     4.1 mutex off
        //     4.2 applicable to CAMPAIGN
        //     4.3 create a few tags under the tag group
        // 5. Tag campaign with any of the 3.3, expect success
        // 6. Tag campaign with additional selection from 3.3, expect failure
        // 7. Tag campaign with any of the 4.3, expect success
        // 8. Tag campaign with additional selection from 4.3, expect success
        Account a = new Account();
        a.setStatus(AccountStatus.ACTIVE);
        a.setStatusUpdated(new Date());
        a.setName("Business Unit");
        mcmServiceDao.createBusinessUnit(a);

        Product p = new Product();

        p.setStatus(ProductStatus.ACTIVE);
        p.setStatusUpdated(new Date());
        p.setId("WDI");
        p.setName("Web Development Immersive OnPrem");
        p= mcmServiceDao.createProduct(p);

        Marketplace m = new Marketplace();
        m.setName("Facebook");
        m.setStatus(MarketplaceStatus.ACTIVE);
        m.setStatusUpdated(new Date());
        m= mcmServiceDao.createMarketplace(m);

        Campaign c = new Campaign();
        c.setBusinessUnit(a.getId());
        c.setProduct(p.getId());
        c.setMarketplace(m.getId());
        c.setTracker(p.getId()+"^drm001");
        c.setName(p.getId() + " " + m.getName() +" SEM direct response marketing in US/NYC Metro");
        c.setStatus(CampaignStatus.ACTIVE);
        c.setStatusUpdated(new Date());
        c.setType(CampaignType.CPC);
        c.setCost(new Long(1));
        c = mcmServiceDao.createCampaign(c);

        TagGroup tg1 = new TagGroup();
        tg1.addApplicableTo(TagType.CAMPAIGN);
        tg1.setMutex(true);
        tg1 = mcmServiceDao.createTagGroup(tg1);

        Tag t11 = new Tag();
        t11.setGroup(tg1.getId());
        t11.setValue("MutexTag1");
        t11 = mcmServiceDao.createTag(t11);

        Tag t12 = new Tag();
        t12.setGroup(tg1.getId());
        t12.setValue("MutexTag2");
        t12 = mcmServiceDao.createTag(t12);


        TagGroup tg2 = new TagGroup();
        tg2.addApplicableTo(TagType.CAMPAIGN);
        tg2.setMutex(false);
        tg2 = mcmServiceDao.createTagGroup(tg2);

        Tag t21 = new Tag();
        t21.setGroup(tg2.getId());
        t21.setValue("NonMutexTag1");
        t21 = mcmServiceDao.createTag(t21);

        Tag t22 = new Tag();
        t22.setGroup(tg2.getId());
        t22.setValue("NonMutexTag2");
        t22 = mcmServiceDao.createTag(t22);

        SortedMap<Tag, SortedSet<Tag>> updatedTags ;
        updatedTags = mcmServiceDao.tagCampaign(c,t11);
        Assert.assertEquals(1,updatedTags.size());
        updatedTags = mcmServiceDao.tagCampaign(c,t12);
        Assert.assertEquals(2,updatedTags.size()); // enforcement of mutex is not a DAO function - service is responsible for it
        updatedTags = mcmServiceDao.tagCampaign(c,t21);
        Assert.assertEquals(3,updatedTags.size());
        updatedTags = mcmServiceDao.tagCampaign(c,t22);
        Assert.assertEquals(4,updatedTags.size());


    }
}
