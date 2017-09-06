package com.stridetech.mcm.dao;
import com.fasterxml.jackson.core.type.TypeReference;
import com.stridetech.mcm.util.*;
import com.stridetech.mcm.model.enums.*;
import com.stridetech.mcm.model.logs.ChangeLog;
import com.stridetech.mcm.model.logs.ChangeLogEntry;
import com.stridetech.mcm.model.logs.UploadLogComparator;
import com.stridetech.mcm.model.meta.*;
import com.stridetech.mcm.model.security.Authority;
import com.stridetech.mcm.model.security.AuthorityType;
import com.stridetech.mcm.model.security.Feed;
import com.stridetech.mcm.model.security.User;
import com.stridetech.mcm.util.*;
import org.jetbrains.annotations.NotNull;
import org.postgresql.util.PGobject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.transaction.annotation.Transactional;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.Date;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.ObjectMapper;

import static java.sql.Statement.RETURN_GENERATED_KEYS;

public class MCMServiceDaoPostgreSQL implements MCMServiceDao {
    private JdbcTemplate jdbcTemplate;
    final ObjectMapper mapper = new ObjectMapper();

    private final Logger LOGGER = LoggerFactory.getLogger(MCMServiceDaoPostgreSQL.class);
    /**
     * @param dataSource
     */
    public void setDatasource(DataSource dataSource) {
        jdbcTemplate = new JdbcTemplate(dataSource);
    }

    /**
     * @param account
     * @return
     */
    @Override
    public Account createBusinessUnit(Account account) {
        KeyHolder keyHolder = new GeneratedKeyHolder();

        PreparedStatementCreator psc = new PreparedStatementCreator() {
            @Override
            public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
                PreparedStatement ps = connection.prepareStatement(
                        " INSERT INTO meta.business_unit(name, description, status, status_updated) " +
                                " VALUES (?, ?, ?::meta.account_status, ?)", RETURN_GENERATED_KEYS);

                int bindIndex = 1;
                ps.setString(bindIndex++, account.getName());
                ps.setString(bindIndex++, account.getDescription());
                ps.setString(bindIndex++, account.getStatus().name());
                ps.setTimestamp(bindIndex, Timestamp.from(account.getStatusUpdated().toInstant()));
                return ps;
            }
        };
        jdbcTemplate.update(psc, keyHolder);
        Account out = keyHolderToAccount(account, keyHolder);
        logAccountStatusChange(out.getId(), out.getStatusUpdated(), out.getStatus());
        return account;
    }

    /**
     * @param account
     * @param keyHolder
     * @return
     */
    public Account keyHolderToAccount(Account account, KeyHolder keyHolder) {
        Map r = keyHolder.getKeys();

        account.setId(Long.valueOf((Integer) r.get("id")));
        account.setName((String) r.get("name"));
        Date status_updated = Date.from(((Timestamp) r.get("status_updated")).toInstant());
        account.setStatusUpdated(status_updated);
        account.setStatus(AccountStatus.valueOf(((PGobject) r.get("status")).getValue()));
        /** TODO: CURRENT IMPLEMENTATION DOES NOT USE ACCOUNT TYPE ENUM **/
        // account.setType(AccountType.valueOf( ((PGobject)r.get("type")).getValue() ));
        account.setType(AccountType.BUSINESS_UNIT);
        account.setDescription((String) r.get("description"));
        // TODO: account.setProducts();
        return account;


    }

    /**
     * @param id
     * @return
     */
    @Override
    public Account retrieveBusinessUnit(Long id) {
        return jdbcTemplate.queryForObject("SELECT id, name, description, status, status_updated FROM meta.business_unit WHERE id = ?",
                new RowMapper<Account>() {
            @Override
            public Account mapRow(ResultSet resultSet, int i) throws SQLException {
                Account account = new Account();
                account.setId(resultSet.getLong("id"));
                account.setName(resultSet.getString("name"));
                account.setDescription(resultSet.getString("description"));
                account.setType(AccountType.BUSINESS_UNIT);
                account.setStatus(AccountStatus.valueOf(resultSet.getString("status")));
                account.setStatusUpdated(resultSet.getTimestamp("status_updated"));
                return account;
            }
        }, id);
    }

    /**
     * @param account
     * @return
     */
    @Override
    public Account updateBusinessUnit(Account account) {
        Account existing = retrieveBusinessUnit(account.getId());
        if (existing.getStatus().compareTo(account.getStatus()) != 0)
            logAccountStatusChange(account.getId(), account.getStatusUpdated(), account.getStatus());

        KeyHolder keyHolder = new GeneratedKeyHolder();
        jdbcTemplate.update(new PreparedStatementCreator() {
            @Override
            public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
                PreparedStatement ps = connection.prepareStatement(
                        "UPDATE meta.business_unit SET name=?, description=?, status=?::meta.account_status, status_updated=? WHERE id=?",
                        Statement.RETURN_GENERATED_KEYS);
                int bindIndex = 1;
                ps.setString(bindIndex++, account.getName());
                ps.setString(bindIndex++, account.getDescription());
                ps.setString(bindIndex++, account.getStatus().name());
                ps.setTimestamp(bindIndex++, Timestamp.from(account.getStatusUpdated().toInstant()));
                ps.setLong(bindIndex, account.getId());
                return ps;
            }
        }, keyHolder);
        return keyHolderToAccount(account, keyHolder);
    }

    /**
     * @param accountId
     * @param effectiveDate
     * @param status
     * @return
     */
    public ChangeLog<AccountStatus> logAccountStatusChange(Long accountId, Date effectiveDate, AccountStatus status) {

        jdbcTemplate.update(new PreparedStatementCreator() {
            @Override
            public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
                PreparedStatement ps =
                        connection.prepareStatement("INSERT INTO logs.account_status_changelog (account,status,effective_date) values (?,?::meta.account_status,?)");
                int bindIndex = 1;
                ps.setLong(bindIndex++, accountId);
                ps.setString(bindIndex++, status.name());
                ps.setTimestamp(bindIndex, Timestamp.from(effectiveDate.toInstant()));
                return ps;
            }
        });


        return getAccountStatusChangelog(accountId);
    }

    /**
     * @param accountId
     * @return
     */
    private ChangeLog<AccountStatus> getAccountStatusChangelog(Long accountId) {
        String query = "SELECT effective_date, status from logs.account_status_changelog where account=?";
        ChangeLog<AccountStatus> changelog = new ChangeLog<>();
        jdbcTemplate.query(new PreparedStatementCreator() {
            @Override
            public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
                PreparedStatement ps = connection.prepareStatement(query);
                ps.setLong(1, accountId);
                return ps;
            }
        }, new RowCallbackHandler() {
            @Override
            public void processRow(ResultSet resultSet) throws SQLException {
                changelog.addLogEntry(new ChangeLogEntry<AccountStatus>(
                        Date.from(resultSet.getTimestamp(1).toInstant()),
                        AccountStatus.valueOf(resultSet.getString(2))
                ));
            }
        });
        return changelog;
    }

    /**
     * @param account
     * @return
     */
    @Override
    public ChangeLog<AccountStatus> getAccountStatusChangelog(Account account) {
        return getAccountStatusChangelog(account.getId());
    }

    /**
     * @param account
     * @return
     */
    @Override
    public Account deleteBusinessUnit(Account account) {
        Account deleted = retrieveBusinessUnit(account.getId());
        deleted.setStatus(AccountStatus.DELETED);
        deleted.setStatusUpdated(new Date());
        return updateBusinessUnit(deleted);
    }

    /**
     * @return
     */
    @Override
    public SortedSet<Account> listBusinessUnits() {
        final String query = "SELECT * FROM meta.business_unit where status <> 'DELETED'::meta.account_status";
        final SortedSet<Account> out = new TreeSet<>(new AccountComparator());
        jdbcTemplate.query(new PreparedStatementCreator() {
            @Override
            public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
                return connection.prepareStatement(query);
            }
        }, new RowCallbackHandler() {
            @Override
            public void processRow(ResultSet resultSet) throws SQLException {
                Account a = new Account();
                a.setId(resultSet.getLong("id"));
                a.setName(resultSet.getString("name"));
                a.setDescription(resultSet.getString("description"));
                a.setStatus(AccountStatus.valueOf(resultSet.getString("status")));
                a.setType(AccountType.BUSINESS_UNIT);
                a.setStatusUpdated(Date.from(resultSet.getTimestamp("status_updated").toInstant()));
                // TODO: add products
                a.setProducts(new TreeSet<String>());
                out.add(a);
            }
        });
        return out;
    }

    @Override
    public SortedSet<Campaign> listBusinessUnitCampaigns(Account account) {
        return listBusinessUnitCampaigns(account.getId());
    }

    public SortedSet<Campaign> listBusinessUnitCampaigns(Long accountId) {
        SortedSet<Campaign> out = new TreeSet<>(new CampaignComparator());
        String query = "SELECT p.* from meta.campaign p JOIN meta.account_campaigns ap ON (p.tracker = ap.tracker) WHERE p.status <> 'DELETED'::meta.campaign_status AND ap.business_unit = ?";
        jdbcTemplate.query(new PreparedStatementCreator() {
            @Override
            public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
                PreparedStatement ps = connection.prepareStatement(query);
                ps.setLong(1, accountId);
                return ps;
            }
        }, new RowCallbackHandler() {
            @Override
            public void processRow(ResultSet resultSet) throws SQLException {
                Campaign p = new Campaign();
                p = resultSetToCampaign(resultSet,p);
                //p.setBusinessUnit(accountId);
                // p.setBusinessUnit(retrieveProductBusinessUnit(p, new Date()).getId());
                out.add(p);
            }
        });
        return out;
    }

    private Product resultSetToProduct(ResultSet resultSet) throws SQLException{
        Product p = new Product();
        p.setId(resultSet.getString("code"));
        p.setName(resultSet.getString("name"));
        p.setDescription(resultSet.getString("description"));
        p.setStatus(ProductStatus.valueOf(resultSet.getString("status")));
        p.setStatusUpdated(Date.from(resultSet.getTimestamp("status_updated").toInstant()));
        return p;
    }
    /**
     * @param effectiveDate
     * @return
     */
    @Override
    public SortedSet<Account> listBusinessUnitsInRetrospect(Date effectiveDate) {

        final String query =
                "SELECT b.id as id, \n" +
                        "       b.name as name, \n" +
                        "       b.description as description, \n" +
                        "       l.status as status, \n" +
                        "       l.status_updated as status_updated \n" +
                        "FROM   meta.business_unit b join (\n" +
                        "    SELECT account as id, \n" +
                        "           status, \n" +
                        "           effective_date as status_updated, \n" +
                        "           rank() OVER( PARTITION BY account ORDER BY effective_date DESC, change_number DESC ) as rank\n" +
                        "    FROM   logs.account_status_changelog \n" +
                        "    WHERE  effective_date <= ?::timestamp without time zone \n" +
                        "    ) l ON b.id = l.id\n" +
                        "    WHERE rank=1\n" +
                        "    AND l.status <> 'DELETED'::meta.account_status";

        final SortedSet<Account> out = new TreeSet<>(new AccountComparator());
        jdbcTemplate.query(new PreparedStatementCreator() {
            @Override
            public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
                PreparedStatement ps = connection.prepareStatement(query);
                ps.setTimestamp(1, Timestamp.from(effectiveDate.toInstant()));
                return ps;
            }
        }, new RowCallbackHandler() {
            @Override
            public void processRow(ResultSet resultSet) throws SQLException {
                Account a = new Account();
                a.setId(resultSet.getLong("id"));
                a.setName(resultSet.getString("name"));
                a.setDescription(resultSet.getString("description"));
                a.setStatus(AccountStatus.valueOf(resultSet.getString("status")));
                a.setType(AccountType.BUSINESS_UNIT);
                a.setStatusUpdated(Date.from(resultSet.getTimestamp("status_updated").toInstant()));
                // TODO: add products
                a.setProducts(new TreeSet<String>());
                out.add(a);
            }
        });
        return out;

    }

    private Boolean isNewAccountNameUnique(String proposedAccountName) {
        final String query = "SELECT COUNT(1) = 0  FROM meta.business_unit  WHERE name=?";
        return jdbcTemplate.queryForObject(query, Boolean.class, proposedAccountName);
    }
    // MARKETPLACE //

    /**
     * @param marketplace
     * @return
     */
    @Override
    public Marketplace createMarketplace(final Marketplace marketplace) {
        KeyHolder keyHolder = new GeneratedKeyHolder();
        jdbcTemplate.update(new PreparedStatementCreator() {
            @Override
            public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
                PreparedStatement ps =
                        connection.prepareStatement("INSERT INTO meta.marketplace(name,description,contact_email,contact_name,status,status_updated) VALUES(?,?,?,?,?::meta.marketplace_status,?)", RETURN_GENERATED_KEYS);

                // TODO: We don't want a marketplace with no name, status or corresponding status effective date
                // So, we should fail fast here with an appropriate exception. No need to go into DB empty handed
                if (marketplace.getName() == null)
                    ps.setNull(1, Types.VARCHAR);
                else
                    ps.setString(1, marketplace.getName());

                if (marketplace.getDescription() == null)
                    ps.setNull(2, Types.VARCHAR);
                else
                    ps.setString(2, marketplace.getDescription());

                if (marketplace.getContactEmail() == null)
                    ps.setNull(3, Types.VARCHAR);
                else
                    ps.setString(3, marketplace.getContactEmail());

                if (marketplace.getContactName() == null)
                    ps.setNull(4, Types.VARCHAR);
                else
                    ps.setString(4, marketplace.getContactName());

                if (marketplace.getStatus() == null)
                    ps.setNull(5, Types.VARCHAR);
                else
                    ps.setString(5,  marketplace.getStatus().name()); // null pointer exception

                if (marketplace.getStatusUpdated()==null)
                    ps.setNull(6, Types.TIMESTAMP );
                else
                    ps.setTimestamp(6,  Timestamp.from(marketplace.getStatusUpdated().toInstant()));

                return ps;
            }
        }, keyHolder);
        Marketplace out = keyHolderToMarketplace(marketplace, keyHolder);
        logMarketplaceStatusChange(marketplace.getId(), marketplace.getStatusUpdated(), marketplace.getStatus());
        return out;
    }

    /**
     * @param marketplace
     * @param keyHolder
     * @return
     */
    private Marketplace keyHolderToMarketplace(Marketplace marketplace, KeyHolder keyHolder) {
        Map r = keyHolder.getKeys();
        marketplace.setId(Long.valueOf((Integer) r.get("id")));
        marketplace.setName((String) r.get("name"));
        marketplace.setDescription((String) r.get("description"));
        marketplace.setStatus(MarketplaceStatus.valueOf(((PGobject) r.get("status")).getValue()));
        marketplace.setStatusUpdated(Date.from(((Timestamp) r.get("status_updated")).toInstant()));
        marketplace.setContactEmail((String) r.get("contact_email"));
        marketplace.setContactName((String) r.get("contact_name"));
        return marketplace;

    }

    /**
     * @param marketplaceId
     * @param effectiveDate
     * @param status
     * @return
     */
    private ChangeLog<MarketplaceStatus> logMarketplaceStatusChange(Long marketplaceId, Date effectiveDate, MarketplaceStatus status) {
        jdbcTemplate.update(new PreparedStatementCreator() {
            @Override
            public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
                PreparedStatement ps =
                        connection.prepareStatement("INSERT INTO logs.marketplace_status_changelog(marketplace,status,effective_date) values (?,?::meta.marketplace_status,?)");
                int bindIndex = 1;
                ps.setLong(bindIndex++, marketplaceId);
                ps.setString(bindIndex++, status.name());
                ps.setTimestamp(bindIndex, Timestamp.from(effectiveDate.toInstant()));
                return ps;
            }
        });
        return getMarketplaceStatusChangelog(marketplaceId);
    }

    /**
     * @param marketplaceId
     * @return
     */
    private ChangeLog<MarketplaceStatus> getMarketplaceStatusChangelog(Long marketplaceId) {
        ChangeLog<MarketplaceStatus> changelog = new ChangeLog<>();
        jdbcTemplate.query(new PreparedStatementCreator() {
            @Override
            public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
                PreparedStatement ps =
                        connection.prepareStatement("SELECT effective_date, status from logs.marketplace_status_changelog where marketplace=?");
                ps.setLong(1, marketplaceId);
                return ps;
            }
        }, new RowCallbackHandler() {
            @Override
            public void processRow(ResultSet resultSet) throws SQLException {
                changelog.addLogEntry(new ChangeLogEntry<MarketplaceStatus>(
                        Date.from(resultSet.getTimestamp(1).toInstant()),
                        MarketplaceStatus.valueOf(resultSet.getString(2))
                ));
            }
        });
        return changelog;
    }

    /**
     * @param marketplace
     * @return
     */
    public ChangeLog<MarketplaceStatus> getMarketplaceStatusChangelog(Marketplace marketplace) {
        return getMarketplaceStatusChangelog(marketplace.getId());
    }

    /**
     * @param id
     * @return
     */
    @Override
    public Marketplace retrieveMarketplace(Long id) {
        return jdbcTemplate.queryForObject("SELECT id, name, description, status, status_updated, contact_name, contact_email FROM meta.marketplace WHERE id = ?", new RowMapper<Marketplace>() {
            @Override
            public Marketplace mapRow(ResultSet resultSet, int i) throws SQLException {
                Marketplace marketplace = new Marketplace();
                marketplace.setId(resultSet.getLong("id"));
                marketplace.setName(resultSet.getString("name"));
                marketplace.setDescription(resultSet.getString("description"));
                marketplace.setStatus(MarketplaceStatus.valueOf(resultSet.getString("status")));
                marketplace.setStatusUpdated(resultSet.getTimestamp("status_updated"));
                marketplace.setContactName(resultSet.getString("contact_name"));
                marketplace.setContactEmail(resultSet.getString("contact_email"));
                return marketplace;
            }
        }, id);
    }

    /**
     * @param marketplace
     * @return
     */
    @Override
    public Marketplace updateMarketplace(Marketplace marketplace) {
        Marketplace existing = retrieveMarketplace(marketplace.getId());
        if (existing.getStatus().compareTo(marketplace.getStatus()) != 0)
            logMarketplaceStatusChange(marketplace.getId(), marketplace.getStatusUpdated(), marketplace.getStatus());
        KeyHolder keyHolder = new GeneratedKeyHolder();
        jdbcTemplate.update(new PreparedStatementCreator() {
            @Override
            public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
                PreparedStatement ps = connection.prepareStatement(
                        "UPDATE meta.marketplace SET name=?, description=?, status=?::meta.marketplace_status, status_updated=?, contact_email=?, contact_name=? WHERE id=?",
                        Statement.RETURN_GENERATED_KEYS
                );
                int bindIndex = 1;
                ps.setString(bindIndex++, marketplace.getName());
                ps.setString(bindIndex++, marketplace.getDescription());
                ps.setString(bindIndex++, marketplace.getStatus().name());
                ps.setTimestamp(bindIndex++, Timestamp.from(marketplace.getStatusUpdated().toInstant()));
                ps.setString(bindIndex++, marketplace.getContactEmail());
                ps.setString(bindIndex++, marketplace.getContactName());
                ps.setLong(bindIndex, marketplace.getId());
                return ps;
            }
        }, keyHolder);
        return keyHolderToMarketplace(marketplace, keyHolder);
    }

    /**
     * @param marketplace
     * @return
     */
    @Override
    public Marketplace deleteMarketplace(Marketplace marketplace) {
        Marketplace deleted = retrieveMarketplace(marketplace.getId());
        deleted.setStatus(MarketplaceStatus.DELETED);
        deleted.setStatusUpdated(new Date());
        return updateMarketplace(deleted);
    }

    /**
     * @return
     */
    @Override
    public SortedSet<Marketplace> listMarketplaces() {
        final SortedSet<Marketplace> out = new TreeSet<>(new MarketplaceComparator());
        jdbcTemplate.query(new PreparedStatementCreator() {
            @Override
            public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
                return connection.prepareStatement("SELECT id, name, description, status, status_updated, contact_email, contact_name from meta.marketplace where status <> 'DELETED'::meta.marketplace_status");

            }
        }, new RowCallbackHandler() {
            @Override
            public void processRow(ResultSet resultSet) throws SQLException {
                Marketplace m = new Marketplace();
                m.setId(resultSet.getLong("id"));
                m.setName(resultSet.getString("name"));
                m.setDescription(resultSet.getString("description"));
                m.setStatus(MarketplaceStatus.valueOf(resultSet.getObject("status").toString()));
                m.setStatusUpdated(Date.from(resultSet.getTimestamp("status_updated").toInstant()));
                m.setContactEmail(resultSet.getString("contact_email"));
                m.setContactName(resultSet.getString("contact_name"));
                out.add(m);
            }
        });
        return out;
    }

    /**
     * @param effectiveDate
     * @return
     */
    @Override
    public SortedSet<Marketplace> listMarketplacesInRetrospect(Date effectiveDate) {
        final SortedSet<Marketplace> out = new TreeSet<>(new MarketplaceComparator());
        final String query =
                "select m.id,\n" +
                        "       m.name,\n" +
                        "       m.description,\n" +
                        "       l.status,\n" +
                        "       l.status_updated,\n" +
                        "       m.contact_email,\n" +
                        "       m.contact_name\n" +
                        "from meta.marketplace m join (\n" +
                        "   select marketplace as id,\n" +
                        "          status,\n" +
                        "          effective_date as status_updated,\n" +
                        "          rank() over(partition by marketplace order by effective_date desc , change_number desc) as rank\n" +
                        "   from logs.marketplace_status_changelog\n" +
                        "   WHERE effective_date <= ?::timestamp without time zone\n" +
                        ") l using (id)\n" +
                        "WHERE rank = 1\n" +
                        "AND l.status <> 'DELETED'::meta.marketplace_status";

        jdbcTemplate.query(new PreparedStatementCreator() {
            @Override
            public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
                PreparedStatement ps = connection.prepareStatement(query);
                ps.setTimestamp(1, Timestamp.from(effectiveDate.toInstant()));
                return ps;
            }
        }, new RowCallbackHandler() {
            @Override
            public void processRow(ResultSet resultSet) throws SQLException {
                Marketplace m = new Marketplace();
                m.setId(resultSet.getLong("id"));
                m.setName(resultSet.getString("name"));
                m.setDescription(resultSet.getString("description"));
                m.setStatus(MarketplaceStatus.valueOf(resultSet.getString("status")));
                m.setStatusUpdated(Date.from(resultSet.getTimestamp("status_updated").toInstant()));
                m.setContactEmail(resultSet.getString("contact_email"));
                m.setContactName(resultSet.getString("contact_name"));
                out.add(m);
            }
        });
        return out;
    }

    public SortedSet<Campaign> listMarketplaceCampaigns(Long marketplaceId) {
        SortedSet<Campaign> out = new TreeSet<>(new CampaignComparator());
        jdbcTemplate.query(new PreparedStatementCreator() {
            @Override
            public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
                PreparedStatement ps = connection.prepareStatement(
                        "SELECT product,tracker,type, name,description,marketplace,status,status_updated " +
                                "FROM meta.campaign " +
                                "JOIN meta.marketplace ON (marketplace.id = campaign.marketplace) " +
                                "JOIN meta.product ON (product.code = campaign.product) "+
                                "WHERE campaign.status <> 'DELETED'::meta.campaign_status " +
                                "AND marketplace.status <> 'DELETED'::meta.marketplace_status " +
                                "AND product.status <> 'DELETED'::meta.product_status "+
                                "AND marketplace.id = ?");
                ps.setLong(1, marketplaceId);
                return ps;
            }
        }, new RowCallbackHandler() {
            @Override
            public void processRow(ResultSet resultSet) throws SQLException {
                Campaign c = new Campaign();
                c.setProduct(resultSet.getString("product"));
                c.setTracker(resultSet.getString("tracker"));
                c.setType(CampaignType.valueOf(resultSet.getString("type")));
                c.setName(resultSet.getString("name"));
                c.setDescription(resultSet.getString("description"));
                c.setMarketplace(resultSet.getLong("marketplace"));
                c.setStatusUpdated(resultSet.getTimestamp("status_udated"));
                out.add(c);
            }
        });
        return out;
    }
    /**
     * @param marketplace
     * @return
     */
    @NotNull
    @Override
    public SortedSet<Campaign> listMarketplaceCampaigns(Marketplace marketplace) {
        return listMarketplaceCampaigns(marketplace.getId());
    }
    @NotNull
    @Override
    public SortedSet<Campaign> listMarketplaceCampaignsInRetrospect(Long marketplaceId, Date effectiveDate) {
        SortedSet<Campaign> out = new TreeSet<>(new CampaignComparator());
        final String query =
                "SELECT c.product,c.tracker,c.type,c.marketplace,c.description,ccl.status,ccl.status_updated\n" +
                        "FROM meta.campaign c\n" +
                        "JOIN meta.marketplace m on (m.id = c.marketplace)\n" +
                        "JOIN meta.product p on (p.code = c.product)\n" +
                        "JOIN (\n" +
                        "      SELECT campaign, status, effective_date as status_updated,\n" +
                        "             rank() over(partition by campaign\n" +
                        "                         order by effective_date desc,\n" +
                        "                                  change_number desc) as rank\n" +
                        "      FROM logs.campaign_status_changelog\n" +
                        "      WHERE effective_date <= ?\n" +
                        "     ) ccl ON ( ccl.campaign = c.tracker )\n" +
                        "JOIN (\n" +
                        "      SELECT product, status, effective_date as status_updated,\n" +
                        "             rank() over(partition by product\n" +
                        "                         order by effective_date desc,\n" +
                        "                                  change_number desc) as rank\n" +
                        "      FROM logs.product_status_changelog\n" +
                        "      WHERE effective_date <= ?\n" +
                        "     ) pcl ON ( pcl.product=c.product)\n" +
                        "JOIN (\n" +
                        "      SELECT marketplace, status, effective_date as status_updated,\n" +
                        "             rank() over(partition by marketplace\n" +
                        "                         order by effective_date desc,\n" +
                        "                                  change_number desc) as rank\n" +
                        "      FROM logs.marketplace_status_changelog\n" +
                        "      WHERE effective_date <= ?\n" +
                        "     ) mcl ON (mcl.marketplace  = c.marketplace)\n" +
                        "WHERE ccl.rank=1 AND ccl.status<>'DELETED'::meta.campaign_status\n" +
                        "AND pcl.rank=1 AND pcl.status<>'DELETED'::meta.product_status\n" +
                        "AND mcl.rank=1 AND mcl.status<>'DELETED'::meta.marketplace_status \n" +
                        "AND m.id = ?";

        jdbcTemplate.query(query, new RowCallbackHandler() {
            @Override
            public void processRow(ResultSet resultSet) throws SQLException {
                out.add(resultSetToCampaign(resultSet,new Campaign()));
            }
        }, effectiveDate, effectiveDate, effectiveDate, marketplaceId);
        return out;
    }
    /**
     * @param marketplace
     * @param effectiveDate
     * @return
     */
    @NotNull
    @Override
    public SortedSet<Campaign> listMarketplaceCampaignsInRetrospect(Marketplace marketplace, Date effectiveDate) {
        return listMarketplaceCampaignsInRetrospect(marketplace.getId(),effectiveDate);
    }

    // CRUD Product //

    /**
     * @param product
     * @return
     */
    @NotNull
    @Override
    @Transactional
    public Product createProduct(Product product) {
        KeyHolder keyHolder = new GeneratedKeyHolder();
        final String query =
                "INSERT INTO meta.product (code, name, description, status, status_updated) values (?,?,?,?::meta.product_status,?)";
        jdbcTemplate.update(new PreparedStatementCreator() {
            @Override
            public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
                PreparedStatement ps = connection.prepareStatement(query, RETURN_GENERATED_KEYS);
                ps.setString(1, product.getId());
                ps.setString(2, product.getName());
                ps.setString(3, product.getDescription());
                ps.setString(4, product.getStatus().name());
                ps.setTimestamp(5, Timestamp.from(product.getStatusUpdated().toInstant()));
                return ps;
            }
        }, keyHolder);
        logProductStatusChange(product);
        return keyHolderToProduct(keyHolder, product);
    }

    public void assignCampaignToAccount(Campaign campaign, Account account) {
        final String queryCheck = "SELECT count(1) > 0 FROM meta.account_campaigns where business_unit = ? and tracker =?";
        final String queryInsert = "INSERT INTO meta.account_campaigns (business_unit, tracker, linked_date) VALUES (?,?,?)";
        final String queryUpdate = "UPDATE meta.account_campaigns SET business_unit=?, linked_date=? WHERE tracker=?";
        Date linked_date = new Date();
        if ( jdbcTemplate.queryForObject(queryCheck,Boolean.class,account.getId(),campaign.getTracker()) ){
            jdbcTemplate.update(queryUpdate, account.getId(), linked_date, campaign.getTracker());
        } else {
            jdbcTemplate.update(queryInsert,account.getId(), campaign.getTracker(), linked_date);
        }
        logCampaignBusinessUnitRelationshipChange(campaign.getTracker(),account.getId(),linked_date);

    }

    public ChangeLog<Account> logCampaignBusinessUnitRelationshipChange(String tracker, Long accountId, Date effectiveDate){
        final String queryInsert = "INSERT INTO logs.campaign_account_relationship_changelog (tracker, account, effective_date) VALUES (?,?,?)";
        final String querySelect = "SELECT account, effective_date FROM logs.campaign_account_relationship_changelog WHERE tracker=?";
        jdbcTemplate.update(new PreparedStatementCreator() {
            @Override
            public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
                PreparedStatement ps = connection.prepareStatement(queryInsert);
                ps.setString(1,tracker);
                ps.setLong(2,accountId);
                ps.setTimestamp(3,Timestamp.from(effectiveDate.toInstant()));
                return ps;
            }
        });
        ChangeLog<Account> out = new ChangeLog<>();
        jdbcTemplate.query(new PreparedStatementCreator() {
            @Override
            public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
                PreparedStatement ps = connection.prepareStatement(querySelect);
                ps.setString(1, tracker);
                return ps;
            }
        }, new RowCallbackHandler() {
            @Override
            public void processRow(ResultSet resultSet) throws SQLException {
                out.addLogEntry(new ChangeLogEntry<>(effectiveDate,retrieveBusinessUnit(accountId)));
            }
        });
        return out;
    }

    /**
     * @param keyHolder
     * @param product
     * @return
     */
    private Product keyHolderToProduct(KeyHolder keyHolder, Product product) {
        Map k = keyHolder.getKeys();
        product.setId((String)k.get("code"));
        product.setName((String)k.get("name"));
        product.setDescription((String)k.get("description"));
        product.setStatus(ProductStatus.valueOf( ((PGobject)k.get("status")).getValue()));
        product.setStatusUpdated(Date.from(((Timestamp) k.get("status_updated")).toInstant()));
        return product;
    }

    /**
     * @param code
     * @return
     */
    @Override
    public Product retrieveProduct(String code) {
        final String query =
                "SELECT code,name,description,status,status_updated FROM meta.product WHERE code=?";
        Product p= jdbcTemplate.queryForObject(query, new RowMapper<Product>() {
            @Override
            public Product mapRow(ResultSet resultSet, int i) throws SQLException {
                return resultSetToProduct(resultSet);
            }
        }, code);

        return p;

    }


    private Account retrieveCampaignBusinessUnit(Campaign campaign, Date effectiveDate){
       return jdbcTemplate.queryForObject(
               "SELECT b.* FROM meta.business_unit b, ( " +
                       "SELECT account FROM logs.campaign_account_relationship_changelog " +
                       "WHERE tracker=? " +
                       "AND effective_date <= ? " +
                       "ORDER by effective_date DESC, change_number DESC " +
                       "LIMIT 1 " +
                       ") l " +
                       " WHERE b.id = l.account", new RowMapper<Account>() {
                   @Override
                   public Account mapRow(ResultSet resultSet, int i) throws SQLException {
                       return resultSetToBusinessUnit(resultSet);
                   }
               }, campaign.getTracker(), effectiveDate);
    }

    private Account resultSetToBusinessUnit(ResultSet resultSet) throws SQLException{
        Account account = new Account();
        account.setId(resultSet.getLong("id"));
        account.setName(resultSet.getString("name"));
        account.setDescription(resultSet.getString("description"));
        account.setType(AccountType.BUSINESS_UNIT);
        account.setStatus(AccountStatus.valueOf(resultSet.getString("status")));
        account.setStatusUpdated(resultSet.getTimestamp("status_updated"));
        return account;
    }
    @Override
    public Product updateProduct(Product product) {
        Product existing = retrieveProduct(product.getId());
        if (existing.getStatus().compareTo(product.getStatus()) != 0) {
            logProductStatusChange(product);
        }
        KeyHolder keyHolder = new GeneratedKeyHolder();
        jdbcTemplate.update(new PreparedStatementCreator() {
            @Override
            public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
                PreparedStatement ps = connection.prepareStatement(
                        "UPDATE meta.product SET name=?, description=?, status=?::meta.product_status, status_updated=? WHERE code=?", RETURN_GENERATED_KEYS
                );
                int bindIndex = 1;
                ps.setString(bindIndex++, product.getName());
                ps.setString(bindIndex++, product.getDescription());
                ps.setString(bindIndex++, product.getStatus().name());
                ps.setTimestamp(bindIndex++, Timestamp.from(product.getStatusUpdated().toInstant()));
                ps.setString(bindIndex, product.getId());
                return ps;
            }
        }, keyHolder);
        return keyHolderToProduct(keyHolder, product);
    }

    /**
     * @param product
     * @return
     */
    public ChangeLog<ProductStatus> logProductStatusChange(Product product) {
        return logProductStatusChange(product.getId(), product.getStatus(), product.getStatusUpdated());
    }

    /**
     * @param code
     * @param status
     * @param effectiveDate
     * @return
     */
    public ChangeLog<ProductStatus> logProductStatusChange(String code, ProductStatus status, Date effectiveDate) {
        jdbcTemplate.update(new PreparedStatementCreator() {
            @Override
            public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
                final String query =
                        "INSERT INTO logs.product_status_changelog (product, status, effective_date) VALUES (?,?::meta.product_status,?)";
                PreparedStatement ps = connection.prepareStatement(query);
                ps.setString(1, code);
                ps.setString(2, status.name());
                ps.setTimestamp(3, Timestamp.from(effectiveDate.toInstant()));
                return ps;
            }
        });
        return getProductStatusChangelog(code);
    }

    /**
     * @param productCode
     * @return
     */
    public ChangeLog<ProductStatus> getProductStatusChangelog(String productCode) {
        ChangeLog<ProductStatus> changelog = new ChangeLog<>();
        jdbcTemplate.query(new PreparedStatementCreator() {
            final String query =
                    "SELECT effective_date, status FROM logs.product_status_changelog WHERE product=?";

            @Override
            public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
                PreparedStatement ps = connection.prepareStatement(query);
                ps.setString(1, productCode);
                return ps;
            }
        }, new RowCallbackHandler() {
            @Override
            public void processRow(ResultSet resultSet) throws SQLException {
                changelog.addLogEntry(new ChangeLogEntry<ProductStatus>(
                        Date.from(resultSet.getTimestamp("effective_date").toInstant()),
                        ProductStatus.valueOf(resultSet.getString("status")))
                );
            }
        });
        return changelog;
    }

    /**
     * @param product
     * @return
     */
    @Override
    public Product deleteProduct(Product product) {
        product = retrieveProduct(product.getId());
        product.setStatusUpdated(new Date());
        product.setStatus(ProductStatus.DELETED);
        return updateProduct(product);
    }

    /**
     * @param product
     * @return
     */
    @Override
    public ChangeLog<ProductStatus> getProductStatusChangelog(Product product) {
        return getProductStatusChangelog(product.getId());
    }

    @Override
    public SortedSet<Product> listProducts() {
        SortedSet<Product> out = new TreeSet<>(new ProductComparator());
        final String query = "SELECT code, name, description, status, status_updated from meta.product WHERE status <> 'DELETED'::meta.product_status";
        jdbcTemplate.query(new PreparedStatementCreator() {
            @Override
            public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
                PreparedStatement ps = connection.prepareStatement(query);
                return ps;
            }
        }, new RowCallbackHandler() {
            @Override
            public void processRow(ResultSet resultSet) throws SQLException {
                Product p = new Product();
                p.setId(resultSet.getString("code"));
                p.setName(resultSet.getString("name"));
                p.setDescription(resultSet.getString("description"));
                p.setStatus(ProductStatus.valueOf(resultSet.getString("status")));
                p.setStatusUpdated(Date.from(resultSet.getTimestamp("status_updated").toInstant()));

                out.add(p);
            }
        });
        return out;
    }

    @Override
    public SortedSet<Product> listProductsInRetrospect(Date effectiveDate) {
        SortedSet<Product> out = new TreeSet<>(new ProductComparator());
        final String query =
                "SELECT p.code as code, " +
                        "       p.name as name, " +
                        "       p.description, " +
                        "       l.status, " +
                        "       l.status_updated " +
                        "FROM   meta.product JOIN ( " +
                        "    SELECT product as code, " +
                        "           status, " +
                        "           effective_date as status_updated, " +
                        "           rank() over(partition by product order by effective_date desc, change_number desc) as rank " +
                        "    FROM  logs.product_status_changelog " +
                        "    WHERE effective_date <=?::timestamp without time zone" +
                        " ) l USING (code) " +
                        "WHERE rank=1 " +
                        "AND   status <> 'DELETED'::meta.product_status";
        jdbcTemplate.query(new PreparedStatementCreator() {
            @Override
            public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
                PreparedStatement ps = connection.prepareStatement(query);
                ps.setTimestamp(1, Timestamp.from(effectiveDate.toInstant()));
                return ps;
            }
        }, new RowCallbackHandler() {
            @Override
            public void processRow(ResultSet resultSet) throws SQLException {
                Product p = new Product();
                p.setId(resultSet.getString("code"));
                p.setName(resultSet.getString("name"));
                p.setDescription(resultSet.getString("description"));
                p.setStatus(ProductStatus.valueOf(resultSet.getString("status")));
                p.setStatusUpdated(Date.from(resultSet.getTimestamp("status_updated").toInstant()));
                // TODO:: p.setBusinessUnit(resultSet.getLong("parent_account"))
                out.add(p);
            }
        });
        return out;
    }

    @NotNull
    @Override
    public SortedSet<Campaign> listProductCampaigns(Product product){
        return listProductCampaigns(product.getId());
    }

    @NotNull
    @Override
    public SortedSet<Campaign> listProductCampaigns(String productCode) {
        SortedSet<Campaign> out = new TreeSet<>(new CampaignComparator());
        jdbcTemplate.query(new PreparedStatementCreator() {
            @Override
            public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
                PreparedStatement ps = connection.prepareStatement(
                        "SELECT product,tracker,type, name,description,marketplace,status,status_updated " +
                                "FROM meta.campaign " +
                                "JOIN meta.marketplace ON (marketplace.id = campaign.marketplace) " +
                                "JOIN meta.product ON (product.code = campaign.product) "+
                                "WHERE campaign.status <> 'DELETED'::meta.campaign_status " +
                                "AND marketplace.status <> 'DELETED'::meta.marketplace_status " +
                                "AND product.status <> 'DELETED'::meta.product_status "+
                                "AND product.code = ?");
                ps.setString(1, productCode);
                return ps;
            }
        }, new RowCallbackHandler() {
            @Override
            public void processRow(ResultSet resultSet) throws SQLException {
                Campaign c = new Campaign();
                c.setProduct(resultSet.getString("product"));
                c.setTracker(resultSet.getString("tracker"));
                c.setType(CampaignType.valueOf(resultSet.getString("type")));
                c.setName(resultSet.getString("name"));
                c.setDescription(resultSet.getString("description"));
                c.setMarketplace(resultSet.getLong("marketplace"));
                c.setStatusUpdated(resultSet.getTimestamp("status_udated"));
                out.add(c);
            }
        });
        return out;
    }

    @NotNull
    private Boolean isNewProductCodeUnique(String proposedProductCode) {
        return jdbcTemplate.queryForObject("SELECT COUNT(1) = 0  FROM meta.product  WHERE code=?", Boolean.class, proposedProductCode);
    }

    @NotNull
    private Boolean isNewProductNameUnique(String proposedProductName) {
        return jdbcTemplate.queryForObject("SELECT COUNT(1) = 0  FROM meta.product  WHERE name=?", Boolean.class, proposedProductName);
    }

    @Override
    public SortedSet<Campaign> listProductCampaignsInRetrospect(Product product, Date effectiveDate) {
        SortedSet<Campaign> out = new TreeSet<>(new CampaignComparator());
        final String query =
                "SELECT c.product,c.tracker,c.type,c.marketplace,c.description,ccl.status,ccl.status_updated\n" +
                        "FROM meta.campaign c\n" +
                        "JOIN meta.marketplace m on (m.id = c.marketplace)\n" +
                        "JOIN meta.product p on (p.code = c.product)\n" +
                        "JOIN (\n" +
                        "      SELECT campaign, status, effective_date as status_updated,\n" +
                        "             rank() over(partition by campaign\n" +
                        "                         order by effective_date desc,\n" +
                        "                                  change_number desc) as rank\n" +
                        "      FROM logs.campaign_status_changelog\n" +
                        "      WHERE effective_date <= ?\n" +
                        "     ) ccl ON ( ccl.campaign = c.tracker )\n" +
                        "JOIN (\n" +
                        "      SELECT product, status, effective_date as status_updated,\n" +
                        "             rank() over(partition by product\n" +
                        "                         order by effective_date desc,\n" +
                        "                                  change_number desc) as rank\n" +
                        "      FROM logs.product_status_changelog\n" +
                        "      WHERE effective_date <= ?\n" +
                        "     ) pcl ON ( pcl.product=c.product)\n" +
                        "JOIN (\n" +
                        "      SELECT marketplace, status, effective_date as status_updated,\n" +
                        "             rank() over(partition by marketplace\n" +
                        "                         order by effective_date desc,\n" +
                        "                                  change_number desc) as rank\n" +
                        "      FROM logs.marketplace_status_changelog\n" +
                        "      WHERE effective_date <= ?\n" +
                        "     ) mcl ON (mcl.marketplace  = c.marketplace)\n" +
                        "WHERE ccl.rank=1 AND ccl.status<>'DELETED'::meta.campaign_status\n" +
                        "AND pcl.rank=1 AND pcl.status<>'DELETED'::meta.product_status\n" +
                        "AND mcl.rank=1 AND mcl.status<>'DELETED'::meta.marketplace_status \n" +
                        "AND p.code = ?";

        jdbcTemplate.query(query, new RowCallbackHandler() {
            @Override
            public void processRow(ResultSet resultSet) throws SQLException {
                out.add(resultSetToCampaign(resultSet,new Campaign()));
            }
        }, effectiveDate, effectiveDate, effectiveDate, product);
        return out;
    }

    @Override
    public ChangeLog<CampaignStatus> getCampaignStatusChangelog(Campaign campaign){
        return getCampaignStatusChangelog(campaign.getTracker());
    }

    @Override
    public ChangeLog<CampaignStatus> getCampaignStatusChangelog(String campaignTracker) {
        ChangeLog<CampaignStatus> changeLog = new ChangeLog<>();
        jdbcTemplate.query(new PreparedStatementCreator() {
            @Override
            public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
                final String query =
                        "SELECT status, effective_date from logs.campaign_status_changelog where campaign=?";
                PreparedStatement ps = connection.prepareStatement(query);
                ps.setString(1, campaignTracker);
                return ps;
            }
        }, new RowCallbackHandler() {
            @Override
            public void processRow(ResultSet resultSet) throws SQLException {
                changeLog.addLogEntry(new ChangeLogEntry<CampaignStatus>(
                        Date.from(resultSet.getTimestamp("effective_date").toInstant()),
                        CampaignStatus.valueOf(resultSet.getString("status"))
                ));
            }
        });
        return changeLog;
    }

    @Override
    public Campaign createCampaign(Campaign campaign) {
        KeyHolder keyHolder = new GeneratedKeyHolder();
        jdbcTemplate.update(new PreparedStatementCreator() {
            @Override
            public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
                PreparedStatement ps = connection.prepareStatement(
                        "INSERT INTO meta.campaign (product, tracker, type, marketplace, name, description, status, status_updated, cost_cents) values (?,?,?::meta.campaign_type,?,?,?,?::meta.campaign_status,?,?)",
                        RETURN_GENERATED_KEYS);
                int bindIndex=0;
                ps.setString(++bindIndex,campaign.getProduct());
                ps.setString(++bindIndex,campaign.getTracker());
                ps.setString(++bindIndex,campaign.getType().name());
                ps.setLong(++bindIndex,campaign.getMarketplace());
                ps.setString(++bindIndex, campaign.getName());
                ps.setString(++bindIndex,campaign.getDescription());
                ps.setString(++bindIndex, campaign.getStatus().name());
                ps.setTimestamp(++bindIndex,Timestamp.from(campaign.getStatusUpdated().toInstant()));

                if (campaign.getCost() != null) {
                    ps.setLong(++bindIndex, campaign.getCost());
                } else {
                    // use setNull
                    ps.setNull(++bindIndex, java.sql.Types.INTEGER);
                }
                return ps;
            }
        }, keyHolder);
        Account account = retrieveBusinessUnit(campaign.getBusinessUnit());
        assignCampaignToAccount(campaign, account);
        logCampaignStatusChange(campaign);
        return keyHolderToCampaign(keyHolder,campaign);
    }

    private Campaign keyHolderToCampaign(KeyHolder keyHolder, Campaign campaign){
        Map k = keyHolder.getKeys();
        campaign.setProduct((String)k.get("product"));
        campaign.setTracker((String)k.get("tracker"));
        campaign.setType(CampaignType.valueOf( ((PGobject)k.get("type")).getValue() ));
        campaign.setMarketplace((Long.valueOf( (Integer)k.get("marketplace") )));
        campaign.setName((String)k.get("name"));
        campaign.setDescription((String)k.get("description"));
        campaign.setStatus(CampaignStatus.valueOf( ((PGobject)k.get("status")).getValue() ));
        campaign.setStatusUpdated(Date.from(((Timestamp) k.get("status_updated")).toInstant()));
        return campaign;
    }

    public ChangeLog<CampaignStatus> logCampaignStatusChange(String campaign, CampaignStatus status, Date effectiveDate ){
        jdbcTemplate.update(new PreparedStatementCreator() {
            @Override
            public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
                final String query =
                        "INSERT INTO logs.campaign_status_changelog (campaign, status, effective_date) VALUES (?,?::meta.campaign_status,?)";
                PreparedStatement ps = connection.prepareStatement(query);
                ps.setString(1, campaign);
                ps.setString(2, status.name());
                ps.setTimestamp(3, Timestamp.from(effectiveDate.toInstant()));
                return ps;
            }
        });
        return  getCampaignStatusChangelog(campaign);
    }

    public ChangeLog<CampaignStatus> logCampaignStatusChange(Campaign campaign){
        return logCampaignStatusChange(campaign.getTracker(), campaign.getStatus(), campaign.getStatusUpdated() );
    }

    @Override
    public Campaign retrieveCampaign(String tracker) {
        final String query=
                "SELECT product, tracker, type, marketplace, name, description, status, status_updated, cost_cents from meta.campaign WHERE tracker=?";
        return jdbcTemplate.queryForObject(query, new RowMapper<Campaign>() {
            @Override
            public Campaign mapRow(ResultSet resultSet, int i) throws SQLException {
                Campaign c = new Campaign();
                c.setProduct(resultSet.getString("product"));
                c.setTracker(resultSet.getString("tracker"));
                c.setType(CampaignType.valueOf(resultSet.getString("type")));
                c.setMarketplace(resultSet.getLong("marketplace"));
                c.setName(resultSet.getString("name"));
                c.setDescription(resultSet.getString("description"));
                c.setStatus(CampaignStatus.valueOf(resultSet.getString("status")));
                c.setStatusUpdated(Date.from(resultSet.getTimestamp("status_updated").toInstant()));
                if (resultSet.getObject("cost_cents") != null)
                    c.setCost(resultSet.getLong("cost_cents"));

                Account a = retrieveCampaignBusinessUnit(c, new Date());
                c.setBusinessUnit(a.getId());

                return c;
            }
        }, tracker);
    }

    @Override
    public Campaign updateCampaign(Campaign campaign) {
        Campaign existing = retrieveCampaign(campaign.getTracker());
        if ( campaign.getStatus().compareTo(existing.getStatus())!=0){
            logCampaignStatusChange(campaign);
        }
        // CAN'T CHANGE PRODUCT OR TRACKER
        // ALSO CAN'T CHANGE TYPE OR COST
        KeyHolder keyHolder = new GeneratedKeyHolder();
        final String query =
                "UPDATE meta.campaign SET name=?, description=?, marketplace=?, status=?::meta.campaign_status, status_updated=? WHERE tracker=?";
        jdbcTemplate.update(new PreparedStatementCreator() {
            @Override
            public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
                PreparedStatement ps = connection.prepareStatement(query, RETURN_GENERATED_KEYS);
                int bindIndex=0;
                ps.setString(++bindIndex,campaign.getName());
                ps.setString(++bindIndex,campaign.getDescription());
                ps.setLong(++bindIndex,campaign.getMarketplace());
                ps.setString(++bindIndex,campaign.getStatus().name());
                ps.setTimestamp(++bindIndex,Timestamp.from(campaign.getStatusUpdated().toInstant()));
                ps.setString(++bindIndex,campaign.getTracker());
                return ps;
            }
        },keyHolder);
        return keyHolderToCampaign(keyHolder,campaign);
    }

    @Override
    public Campaign deleteCampaign(Campaign campaign) {
        Campaign deleted = retrieveCampaign(campaign.getTracker());
        deleted.setStatus(CampaignStatus.DELETED);
        deleted.setStatusUpdated(new Date());
        return updateCampaign(deleted);
    }

    @Override
    public SortedSet<Campaign> listCampaigns() {
        SortedSet<Campaign> out =new TreeSet<>(new CampaignComparator());
        jdbcTemplate.query(
            "SELECT product,tracker,type,marketplace,name,description,status,status_updated,cost_cents FROM meta.campaign WHERE status<>'DELETED'::meta.campaign_status",
            new RowCallbackHandler() {
            @Override
            public void processRow(ResultSet resultSet) throws SQLException {
                Campaign c = new Campaign();
                c.setProduct(resultSet.getString("product"));
                c.setTracker(resultSet.getString("tracker"));
                c.setType(CampaignType.valueOf(resultSet.getString("type")));
                c.setMarketplace(resultSet.getLong("marketplace"));
                c.setName(resultSet.getString("name"));
                c.setDescription(resultSet.getString("description"));
                c.setStatus(CampaignStatus.valueOf(resultSet.getString("status")));
                c.setStatusUpdated(resultSet.getTimestamp("status_updated"));
                c.setCost(resultSet.getLong("cost_cents"));

                Account a = retrieveCampaignBusinessUnit(c,new Date());
                c.setBusinessUnit(a.getId());
                out.add(c);
            }
        });
        return out;
    }

    @Override
    public SortedSet<Campaign> listCampaignsInRetrospect(Date effectiveDate) {
        SortedSet<Campaign> out = new TreeSet<>(new CampaignComparator());
        final String query =
                "SELECT c.product,c.tracker,c.type,c.marketplace,c.description,ccl.status,ccl.status_updated,c.cost_cents\n" +
                        "FROM meta.campaign c\n" +
                        "JOIN meta.marketplace m on (m.id = c.marketplace)\n" +
                        "JOIN meta.product p on (p.code = c.product)\n" +
                        "JOIN (\n" +
                        "      SELECT campaign, status, effective_date as status_updated,\n" +
                        "             rank() over(partition by campaign\n" +
                        "                         order by effective_date desc,\n" +
                        "                                  change_number desc) as rank\n" +
                        "      FROM logs.campaign_status_changelog\n" +
                        "      WHERE effective_date <= ?\n" +
                        "     ) ccl ON ( ccl.campaign = c.tracker )\n" +
                        "JOIN (\n" +
                        "      SELECT product, status, effective_date as status_updated,\n" +
                        "             rank() over(partition by product\n" +
                        "                         order by effective_date desc,\n" +
                        "                                  change_number desc) as rank\n" +
                        "      FROM logs.product_status_changelog\n" +
                        "      WHERE effective_date <= ?\n" +
                        "     ) pcl ON ( pcl.product=c.product)\n" +
                        "JOIN (\n" +
                        "      SELECT marketplace, status, effective_date as status_updated,\n" +
                        "             rank() over(partition by marketplace\n" +
                        "                         order by effective_date desc,\n" +
                        "                                  change_number desc) as rank\n" +
                        "      FROM logs.marketplace_status_changelog\n" +
                        "      WHERE effective_date <= ?\n" +
                        "     ) mcl ON (mcl.marketplace  = c.marketplace)\n" +
                        "WHERE ccl.rank=1 AND ccl.status<>'DELETED'::meta.campaign_status\n" +
                        "AND pcl.rank=1 AND pcl.status<>'DELETED'::meta.product_status\n" +
                        "AND mcl.rank=1 AND mcl.status<>'DELETED'::meta.marketplace_status";

        jdbcTemplate.query(query, new RowCallbackHandler() {
            @Override
            public void processRow(ResultSet resultSet) throws SQLException {
                out.add(resultSetToCampaign(resultSet,new Campaign()));
            }
        }, effectiveDate, effectiveDate, effectiveDate);
        return out;
    }
    private Campaign resultSetToCampaign(ResultSet resultSet, Campaign campaign) throws SQLException{
        campaign.setProduct(resultSet.getString("product"));
        campaign.setTracker(resultSet.getString("tracker"));
        campaign.setType(CampaignType.valueOf(resultSet.getString("type")));
        campaign.setMarketplace(resultSet.getLong("marketplace"));
        campaign.setDescription(resultSet.getString("description"));
        campaign.setStatus(CampaignStatus.valueOf(resultSet.getString("status")));
        campaign.setStatusUpdated(Date.from(resultSet.getTimestamp("status_updated").toInstant())); // null pointer exception here
        campaign.setCost(resultSet.getLong("cost_cents"));
        return campaign;
    }

    /**
     * Unique object ( for update or insertion )
     * not myself, same name or id or whatever else must be unique
     *
     * @param marketplace
     */
    @Override
    public Boolean isNewMarketplaceUnique(Marketplace marketplace, Boolean doExcludeOwnId) {
        final String query;
        if (doExcludeOwnId ){
            // UPDATE - Trying to change name on existing key
            return jdbcTemplate.queryForObject("SELECT COUNT(1) = 0  FROM meta.marketplace  WHERE name=? and id <>?",
                    Boolean.class, marketplace.getName(), marketplace.getId() );
        } else {
            // INSERT - Trying to insert new name and get a unique key
            return jdbcTemplate.queryForObject("SELECT COUNT(1) = 0  FROM meta.marketplace  WHERE name=?",
                    Boolean.class, marketplace.getName() );
        }

    }

    @Override
    public Boolean isNewProductUnique(Product product, Boolean doExcludeOwnId) {
        if (doExcludeOwnId ){
            // UPDATE - Trying to change name on existing key
            return jdbcTemplate.queryForObject("SELECT COUNT(1) = 0  FROM meta.product  WHERE name=? and code <>?",
                    Boolean.class, product.getName(), product.getId() );
        } else {
            // INSERT - Trying to insert new name and get a unique key
            return jdbcTemplate.queryForObject("SELECT COUNT(1) = 0  FROM meta.product  WHERE name=? OR code=?",
                    Boolean.class, product.getName(), product.getId() );
        }
    }

    @Override
    public Boolean isNewCampaignUnique(Campaign campaign, Boolean doExcludeOwnId) {
        if (doExcludeOwnId ){
            // UPDATE - Trying to change name on existing key
            return jdbcTemplate.queryForObject("SELECT COUNT(1) = 0  FROM meta.campaign  WHERE (name=? AND product=?) AND tracker <>?",
                    Boolean.class, campaign.getName(), campaign.getProduct(), campaign.getTracker() );
        } else {
            // INSERT - Trying to insert new name and get a unique key
            return jdbcTemplate.queryForObject("SELECT COUNT(1) = 0  FROM meta.campaign  WHERE (name=? AND product=?) OR tracker=?",
                    Boolean.class, campaign.getName(), campaign.getProduct(), campaign.getTracker() );
        }
    }

    @Override
    public Boolean isNewAccountUnique(Account account,  Boolean doExcludeOwnId) {
        if (doExcludeOwnId ){
            // UPDATE - Trying to change name on existing key
            return jdbcTemplate.queryForObject("SELECT COUNT(1) = 0  FROM meta.business_unit  WHERE name=? and id <>?",
                    Boolean.class, account.getName(), account.getId() );
        } else {
            // INSERT - Trying to insert new name and get a unique key
            return jdbcTemplate.queryForObject("SELECT COUNT(1) = 0  FROM meta.business_unit  WHERE name=?",
                    Boolean.class, account.getName());
        }
    }

    // TODO: implement tag methods
    @Override
    public TagGroup createTagGroup(TagGroup tagGroup)  {
        KeyHolder keyHolder = new GeneratedKeyHolder();

        PreparedStatementCreator psc = new PreparedStatementCreator() {
            @Override
            public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
                PreparedStatement ps = connection.prepareStatement(" INSERT INTO meta.tag_group(is_mutex, applicable_to) VALUES (?, ?)", RETURN_GENERATED_KEYS);

                ps.setBoolean(1, tagGroup.getMutex());
                String[] applicability = new String[tagGroup.getApplicableTo().size()];
                int i = 0;
                for (TagType tagType : tagGroup.getApplicableTo()) {
                    applicability[i++] = tagType.name();
                }
                ps.setArray(2, connection.createArrayOf("meta.tag_type", applicability));

                return ps;
            }
        };
        jdbcTemplate.update(psc, keyHolder);
        try {
            TagGroup out = keyHolderToTagGroup(tagGroup, keyHolder);
        } catch (SQLException e){
            LOGGER.error("Can't get object from key holder", e);
            return null;
        }
        return tagGroup;
    }

    public TagGroup keyHolderToTagGroup(TagGroup tagGroup, KeyHolder keyHolder) throws SQLException {
        Map r = keyHolder.getKeys();

        tagGroup.setId(Long.valueOf((Integer) r.get("id")));
        tagGroup.setMutex((Boolean) r.get("is_mutex"));

       Array arr =  (Array) r.get("applicable_to");
       Object[] o= (Object[]) arr.getArray();

        tagGroup.setApplicableTo(new TreeSet<TagType>());
        Arrays.stream(o).forEach(
                tagType -> {
                    tagGroup.getApplicableTo()
                            .add(TagType.valueOf(((PGobject)tagType).getValue()));
                }
        );
        return  tagGroup;

    }

    @Override
    public TagGroup retrieveTagGroup(Long id) {
        return jdbcTemplate.queryForObject("SELECT id, is_mutex, applicable_to FROM meta.tag_group WHERE id = ?",
                (resultSet, i) -> resultSetToTagGroup(resultSet), id);
    }

    @Override
    public TagGroup updateTagGroup(TagGroup tagGroup) {
        return null;
    }

    @Override
    public TagGroup deleteTagGroup(TagGroup tagGroup) {
        return null;
    }

    @Override
    public SortedSet<TagGroup> allApplicable(TagType type) {
        SortedSet<TagGroup> out = new TreeSet<>(new TagGroupComparator());
        final String query =
                "SELECT tag_group.* FROM meta.tag_group WHERE applicable_to=ANY(?)";
        jdbcTemplate.query(query, new RowCallbackHandler() {
            @Override
            public void processRow(ResultSet resultSet) throws SQLException {
                out.add(resultSetToTagGroup(resultSet));
            }
        }, type.name());
        return out;
    }

    private TagGroup resultSetToTagGroup(ResultSet resultSet) throws SQLException{
        TagGroup tg = new TagGroup();
        tg.setId(resultSet.getLong("id"));
        tg.setMutex(resultSet.getBoolean("is_mutex"));
        Object[] tags =  (Object[]) resultSet.getArray("applicable_to").getArray();
        tg.setApplicableTo( new TreeSet<>() );
        for (Object t: tags
             ) {
            tg.addApplicableTo(TagType.valueOf(((PGobject)t).getValue()));
        }
        return tg;
    }


    @Override
    public Tag createTag(Tag tag) {
        KeyHolder keyHolder = new GeneratedKeyHolder();

        PreparedStatementCreator psc = new PreparedStatementCreator() {
            @Override
            public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
                PreparedStatement ps =
                        connection.prepareStatement(" INSERT INTO meta.tag(\"group\", value) VALUES (?, ?)", RETURN_GENERATED_KEYS);
                ps.setLong(1, tag.getGroup());
                ps.setString(2, tag.getValue());
                return ps;
            }
        };
        jdbcTemplate.update(psc, keyHolder);
        try {
            Tag out = keyHolderToTag(tag, keyHolder);
        } catch (SQLException e){
            LOGGER.error("Can't get object from key holder", e);
            return null;
        }
        return tag;
    }

    public Tag keyHolderToTag(Tag tag, KeyHolder keyHolder) throws SQLException {
        Map r = keyHolder.getKeys();
        tag.setId(Long.valueOf((Integer) r.get("id")));
        tag.setValue((String) r.get("value"));
        tag.setGroup(Long.valueOf((Integer) r.get("group"))) ;
        return  tag;
    }

    @Override
    public Tag retrieveTag(Long id) {
        return null;
    }

    @Override
    public Tag updateTag(Tag tag) {
        return null;
    }

    @Override
    public Tag deleteTag(Tag tag) {
        return null;
    }

    @Override
    public SortedMap<Tag, SortedSet<Tag>> listCampaignTags(Campaign campaign) {
        return listCampaignTags(campaign.getTracker());
    }

    @Override
    public SortedMap<Tag, SortedSet<Tag>> listCampaignTags(String tracker) {
        SortedMap<Tag,SortedSet<Tag>> out = new TreeMap<>(new TagComparator());
        final String query =
                        "SELECT tag.id, tag.value,tag.\"group\", \n" +
                        "  COALESCE( json_agg(exclusions) FILTER (WHERE exclusions.id IS NOT NULL) , '[]') as exclusions\n" +
                        "FROM  meta.campaign \n" +
                        "  JOIN meta.campaign_tags \n" +
                        "    ON (campaign.tracker = campaign_tags.campaign) \n" +
                        "  JOIN meta.tag \n" +
                        "    ON (campaign_tags.tag = tag.id) \n" +
                        "  JOIN meta.tag_group \n" +
                        "    ON (tag.\"group\" = tag_group.id) \n" +
                        "  LEFT OUTER JOIN meta.tag AS exclusions \n" +
                        "    ON (tag_group.id = exclusions.\"group\" and tag_group.is_mutex and exclusions.id <> tag.id ) \n" +
                        "WHERE campaign.tracker=? \n" +
                        "GROUP BY 1,2,3; ";
        jdbcTemplate.query(query, new RowCallbackHandler() {
            @Override
            public void processRow(ResultSet resultSet) throws SQLException {
                Tag tag = new Tag();
                tag.setGroup( resultSet.getLong("group"));
                tag.setId(resultSet.getLong("id"));
                tag.setValue(resultSet.getString("value"));
                try {
                    String jsonString = resultSet.getString("exclusions");
                    TreeSet<Tag> exclusions = mapper.readValue(jsonString, new TypeReference<TagTreeSet>(){});
                    out.put(tag,exclusions);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }, tracker);
        return out;
    }

    @Override
    public SortedMap<Tag, SortedSet<Tag>> listProductTags(Product product) {
        return listProductTags(product.getId());
    }

    @Override
    public SortedMap<Tag, SortedSet<Tag>> listProductTags(String code) {
        SortedMap<Tag,SortedSet<Tag>> out = new TreeMap<>(new TagComparator());
        final String query =
                "SELECT tag.id, tag.value,tag.\"group\", \n" +
                        "  COALESCE( json_agg(exclusions) FILTER (WHERE exclusions.id IS NOT NULL) , '[]') as exclusions\n" +
                        "FROM  meta.product \n" +
                        "  JOIN meta.product_tags \n" +
                        "    ON (product.code = product_tags.product) \n" +
                        "  JOIN meta.tag \n" +
                        "    ON (product_tags.tag = tag.id) \n" +
                        "  JOIN meta.tag_group \n" +
                        "    ON (tag.\"group\" = tag_group.id) \n" +
                        "  LEFT OUTER JOIN meta.tag AS exclusions \n" +
                        "    ON (tag_group.id = exclusions.\"group\" and tag_group.is_mutex and exclusions.id <> tag.id ) \n" +
                        "WHERE product.code=? \n" +
                        "GROUP BY 1,2,3; ";
        jdbcTemplate.query(query, new RowCallbackHandler() {
            @Override
            public void processRow(ResultSet resultSet) throws SQLException {
                Tag tag = new Tag();
                tag.setGroup( resultSet.getLong("group"));
                tag.setId(resultSet.getLong("id"));
                tag.setValue(resultSet.getString("value"));
                try {
                    String jsonString = resultSet.getString("exclusions");
                    TreeSet<Tag> exclusions = mapper.readValue(jsonString, new TypeReference<TagTreeSet>(){});
                    out.put(tag,exclusions);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }, code);
        return out;
    }

    @Override
    public SortedMap<Tag, SortedSet<Tag>> listAccountTags(Account account) {
        return listAccountTags(account.getId());
    }

    @Override
    public SortedMap<Tag, SortedSet<Tag>> listAccountTags(Long id) {
        SortedMap<Tag,SortedSet<Tag>> out = new TreeMap<>(new TagComparator());
        final String query =
                "SELECT tag.id, tag.value,tag.\"group\", \n" +
                        "  COALESCE( json_agg(exclusions) FILTER (WHERE exclusions.id IS NOT NULL) , '[]') as exclusions\n" +
                        "FROM  meta.business_unit \n" +
                        "  JOIN meta.account_tags \n" +
                        "    ON (business_unit.id = account_tags.account) \n" +
                        "  JOIN meta.tag \n" +
                        "    ON (account_tags.tag = tag.id) \n" +
                        "  JOIN meta.tag_group \n" +
                        "    ON (tag.\"group\" = tag_group.id) \n" +
                        "  LEFT OUTER JOIN meta.tag AS exclusions \n" +
                        "    ON (tag_group.id = exclusions.\"group\" and tag_group.is_mutex and exclusions.id <> tag.id ) \n" +
                        "WHERE business_unit.id = ? \n" +
                        "GROUP BY 1,2,3; ";
        jdbcTemplate.query(query, new RowCallbackHandler() {
            @Override
            public void processRow(ResultSet resultSet) throws SQLException {
                Tag tag = new Tag();
                tag.setGroup( resultSet.getLong("group"));
                tag.setId(resultSet.getLong("id"));
                tag.setValue(resultSet.getString("value"));
                try {
                    String jsonString = resultSet.getString("exclusions");
                    TreeSet<Tag> exclusions = mapper.readValue(jsonString, new TypeReference<TagTreeSet>(){});
                    out.put(tag,exclusions);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }, id);
        return out;
    }

    @Override
    public SortedMap<Tag, SortedSet<Tag>> listMarketplaceTags(Marketplace marketplace) {
        return listMarketplaceTags(marketplace.getId());
    }

    @Override
    public SortedMap<Tag, SortedSet<Tag>> listMarketplaceTags(Long id) {
        SortedMap<Tag,SortedSet<Tag>> out = new TreeMap<>(new TagComparator());
        final String query =
                "SELECT tag.id, tag.value,tag.\"group\", \n" +
                        "  COALESCE( json_agg(exclusions) FILTER (WHERE exclusions.id IS NOT NULL) , '[]') as exclusions\n" +
                        "FROM  meta.marketplace \n" +
                        "  JOIN meta.marketplace_tags \n" +
                        "    ON (marketplace.id = marketplace_tags.marketplace) \n" +
                        "  JOIN meta.tag \n" +
                        "    ON (marketplace_tags.tag = tag.id) \n" +
                        "  JOIN meta.tag_group \n" +
                        "    ON (tag.\"group\" = tag_group.id) \n" +
                        "  LEFT OUTER JOIN meta.tag AS exclusions \n" +
                        "    ON (tag_group.id = exclusions.\"group\" and tag_group.is_mutex and exclusions.id <> tag.id ) \n" +
                        "WHERE marketplace.id = ? \n" +
                        "GROUP BY 1,2,3; ";
        jdbcTemplate.query(query, new RowCallbackHandler() {
            @Override
            public void processRow(ResultSet resultSet) throws SQLException {
                Tag tag = new Tag();
                tag.setGroup( resultSet.getLong("group"));
                tag.setId(resultSet.getLong("id"));
                tag.setValue(resultSet.getString("value"));
                try {
                    String jsonString = resultSet.getString("exclusions");
                    TreeSet<Tag> exclusions = mapper.readValue(jsonString, new TypeReference<TagTreeSet>(){});
                    out.put(tag,exclusions);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }, id);
        return out;
    }

    @Override
    public SortedMap<Tag, SortedSet<Tag>> tagCampaign(Campaign campaign, Tag tag) {
        jdbcTemplate.update(new PreparedStatementCreator() {
            @Override
            public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
                PreparedStatement ps =
                        connection.prepareStatement("INSERT INTO meta.campaign_tags (campaign,tag) values (?,?)");

                ps.setString(1, campaign.getTracker());
                ps.setLong(2, tag.getId());

                return ps;
            }
        });
        return listCampaignTags(campaign);
    }

    @Override
    public SortedMap<Tag, SortedSet<Tag>> untagCampaign(Campaign campaign, Tag tag) {
        jdbcTemplate.update(new PreparedStatementCreator() {
            @Override
            public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
                PreparedStatement ps =
                        connection.prepareStatement("DELETE FROM meta.campaign_tags WHERE campaign=? AND tag=?");

                ps.setString(1, campaign.getTracker());
                ps.setLong(2, tag.getId());

                return ps;
            }
        });
        return listCampaignTags(campaign);
    }

    @Override
    public SortedMap<Tag, SortedSet<Tag>> tagAccount(Account account, Tag tag) {
        jdbcTemplate.update(new PreparedStatementCreator() {
            @Override
            public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
                PreparedStatement ps =
                        connection.prepareStatement("INSERT INTO meta.account_tags (account,tag) values (?,?)");

                ps.setLong(1, account.getId());
                ps.setLong(2, tag.getId());

                return ps;
            }
        });
        return listAccountTags(account);
    }

    @Override
    public SortedMap<Tag, SortedSet<Tag>> untagAccount(Account account, Tag tag) {
        jdbcTemplate.update(new PreparedStatementCreator() {
            @Override
            public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
                PreparedStatement ps =
                        connection.prepareStatement("DELETE FROM meta.account_tags WHERE account=? AND tag=?");

                ps.setLong(1, account.getId());
                ps.setLong(2, tag.getId());

                return ps;
            }
        });
        return listAccountTags(account);
    }

    @Override
    public SortedMap<Tag, SortedSet<Tag>> tagProduct(Product product, Tag tag) {
        jdbcTemplate.update(new PreparedStatementCreator() {
            @Override
            public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
                PreparedStatement ps =
                        connection.prepareStatement("INSERT INTO meta.product_tags (product,tag) values (?,?)");

                ps.setString(1, product.getId());
                ps.setLong(2, tag.getId());

                return ps;
            }
        });
        return listProductTags(product);
    }

    @Override
    public SortedMap<Tag, SortedSet<Tag>> untagProduct(Product product, Tag tag) {
        jdbcTemplate.update(new PreparedStatementCreator() {
            @Override
            public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
                PreparedStatement ps =
                        connection.prepareStatement("DELETE FROM meta.product_tags where product=? AND tag=?");

                ps.setString(1, product.getId());
                ps.setLong(2, tag.getId());

                return ps;
            }
        });
        return listProductTags(product);
    }

    @Override
    public SortedMap<Tag, SortedSet<Tag>> tagMarketplace(Marketplace marketplace, Tag tag) {
        jdbcTemplate.update(new PreparedStatementCreator() {
            @Override
            public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
                PreparedStatement ps =
                        connection.prepareStatement("INSERT INTO meta.marketplace_tags (marketplace,tag) values (?,?)");

                ps.setLong(1, marketplace.getId());
                ps.setLong(2, tag.getId());

                return ps;
            }
        });
        return listMarketplaceTags(marketplace);
    }

    @Override
    public SortedMap<Tag, SortedSet<Tag>> untagMarketplace(Marketplace marketplace, Tag tag) {
        jdbcTemplate.update(new PreparedStatementCreator() {
            @Override
            public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
                PreparedStatement ps =
                        connection.prepareStatement("DELETE FROM meta.marketplace_tags WHERE marketplace=?  AND tag=?");

                ps.setLong(1, marketplace.getId());
                ps.setLong(2, tag.getId());

                return ps;
            }
        });
        return listMarketplaceTags(marketplace);
    }

    @Override
    public SortedSet<Tag> retrieveTagGroupTags(Long tagGroupId) {
        SortedSet<Tag> out = new TreeSet<>(new TagComparator());
        final String query =
                "SELECT tag.* FROM meta.tag WHERE \"group\" = ?";
        jdbcTemplate.query(query, new RowCallbackHandler() {
            @Override
            public void processRow(ResultSet resultSet) throws SQLException {
                out.add(resultSetToTag(resultSet));
            }
        }, tagGroupId);
        return out;
    }

    private Tag resultSetToTag(ResultSet resultSet) throws SQLException{
        Tag tag = new Tag();
        tag.setId(resultSet.getLong("id"));
        tag.setGroup(resultSet.getLong("group"));
        tag.setValue(resultSet.getString("value"));
        return tag;
    }

    @Override
    public SortedSet<Tag> retrieveTagGroupTags(TagGroup tagGroup) {
        return retrieveTagGroupTags(tagGroup.getId());
    }

    @Override
    public SortedSet<TagGroup> listTagGroups() {
        final SortedSet<TagGroup> out = new TreeSet<>(new TagGroupComparator());
        jdbcTemplate.query(new PreparedStatementCreator() {
            @Override
            public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
                return connection.prepareStatement("SELECT id, is_mutex, applicable_to from meta.tag_group");
            }
        }, new RowCallbackHandler() {
            @Override
            public void processRow(ResultSet resultSet) throws SQLException {
               TagGroup t = resultSetToTagGroup(resultSet);
                out.add(t);
            }
        });
        return out;
    }

    /**
     * Inner class to fix jackson databind behaviour
     */
    static class TagTreeSet extends TreeSet<Tag> {
        public TagTreeSet() {
            super(new TagComparator());
        }
    }


    private Metro resultSetToMetro(ResultSet resultSet) throws SQLException{
        Metro metro= new Metro();
        metro.setId(resultSet.getLong("id"));
        metro.setName(resultSet.getString("name"));
        metro.setDescription(resultSet.getString("description"));
        metro.setExtended(resultSet.getBoolean("extended"));
        return metro;
    }

    @Override
    public SortedSet<Metro> listMetros() {

        SortedSet<Metro> out = new TreeSet<>(new MetroComparator());
        final String query =
                "SELECT metro.* FROM meta.metro";
        jdbcTemplate.query(query, new RowCallbackHandler() {
            @Override
            public void processRow(ResultSet resultSet) throws SQLException {
                out.add(resultSetToMetro(resultSet));
            }
        });
        return out;
    }

    @Override
    public Metro retrieveMetro(Long id) {
        return jdbcTemplate.queryForObject("SELECT metro.* FROM meta.metro WHERE id = ?",
            new RowMapper<Metro>() {
            @Override
            public Metro mapRow(ResultSet resultSet, int i) throws SQLException {
                Metro metro = new Metro();
                metro.setId(resultSet.getLong("id"));
                metro.setName(resultSet.getString("name"));
                metro.setDescription(resultSet.getString("description"));
                return metro;
            }
        }, id);

    }

    @Override
    public Metro setCampaignMetro(Campaign campaign, Metro metro) {
        jdbcTemplate.update(new PreparedStatementCreator() {
            @Override
            public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
                PreparedStatement ps =
                        connection.prepareStatement(
                        " INSERT INTO meta.campaign_metros(tracker, metro) VALUES (?, ?)" );

                ps.setString(1, campaign.getTracker());
                ps.setLong(2, metro.getId());

                return ps;
            }
        });
        return metro;
    }

    @Override
    public Metro unsetCampaignMetro(Campaign campaign, Metro metro) {
        jdbcTemplate.update(new PreparedStatementCreator() {
            @Override
            public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
                PreparedStatement ps =
                        connection.prepareStatement(
                        " DELETE FROM meta.campaign_metros WHERE tracker=? AND metro=? ");

                ps.setString(1, campaign.getTracker());
                ps.setLong(2, metro.getId());

                return ps;
            }
        });
        return metro;
    }

    @Override
    public SortedSet<Metro> listCampaignMetros(Campaign campaign) {
        SortedSet<Metro> out = new TreeSet<>(new MetroComparator());
        final String query =
                "SELECT metro.id, metro.name, metro.description \n" +
                "FROM  meta.campaign_metros \n" +
                "  JOIN meta.metro \n" +
                "    ON (campaign_metros.metro = metro.id) \n" +
                "WHERE campaign_metros.tracker = ?";
        jdbcTemplate.query(query, new RowCallbackHandler() {
            @Override
            public void processRow(ResultSet resultSet) throws SQLException {
                Metro metro = new Metro();
                metro.setId(resultSet.getLong("id"));
                metro.setName( resultSet.getString("name"));
                metro.setDescription(resultSet.getString("description"));
                out.add(metro);
            }
        }, campaign.getTracker());
        return out;
    }


    @Override
    public SortedSet<Feed> listFeedDef(String user, boolean summary) {
        final String query = "select * from public.feed_def fd\n" +
                "join public.feed_authority fa on fa.id_feed = fd.id\n" +
                "join public.users u on u.id = fa.id_user\n" +
                "where u.email = '" + user + "'";
        final SortedSet<Feed> out = new TreeSet<>(new UploadTypeComparator());
        jdbcTemplate.query(new PreparedStatementCreator() {
            @Override
            public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
                return connection.prepareStatement(query);
            }
        }, new RowCallbackHandler() {
            @Override
            public void processRow(ResultSet resultSet) throws SQLException {
                Feed a = new Feed();
                a.setId(resultSet.getLong("id"));
                a.setName(resultSet.getString("name"));
                if (!summary){
                    a.setPrefix(resultSet.getString("prefix"));
                    a.setBucket(resultSet.getString("bucket"));
                    a.setRejectPrefix(resultSet.getString("reject_prefix"));
                    a.setArchivePrefix(resultSet.getString("archive_prefix"));
                }

                out.add(a);
            }
        });
        return out;
    }

    @Override
    public Feed getFeed(Long fileType){

        final String query = "SELECT * FROM public.feed_def where id = " + fileType;


        return jdbcTemplate.queryForObject(query, new RowMapper<Feed>() {
            @Override
            public Feed mapRow(ResultSet resultSet, int i) throws SQLException {
                Feed type = new Feed();
                type.setBucket(resultSet.getString("bucket"));
                type.setPrefix(resultSet.getString("prefix"));
                type.setFilePrefix(resultSet.getString("file_prefix"));
                return type;
            }
        });
    }


    @Override
    public User getUserByEmail(String email){

        final String query = "select u.email,u.enabled, a.name as authority, null as feed \n" +
                "from public.users u\n" +
                "left join users_authority ua on ua.id_user = u.id\n" +
                "left join authority a on ua.id_authority = a.id\n" +
                "where email = '" + email + "'" +
                "union \n" +
                "select u.email,u.enabled, null as authority, fd.name as feed\n" +
                "from public.users u\n" +
                "join users_authority ua on ua.id_user = u.id\n" +
                "join authority a on ua.id_authority = a.id and a.name = 'feeds'\n" +
                "join feed_authority fa on fa.id_user = u.id\n" +
                "join feed_def fd on fa.id_feed = fd.id\n" +
                "where email = '" + email + "'";


        List<Authority> authorities = new ArrayList<>();
        List<Feed> feeds = new ArrayList<>();
        final User user = new User();
        user.setEmail(email);
        jdbcTemplate.query(new PreparedStatementCreator() {
            @Override
            public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
                return connection.prepareStatement(query);
            }
        }, new RowCallbackHandler() {
            @Override
            public void processRow(ResultSet resultSet) throws SQLException {
                if (user.getEnabled() == null) user.setEnabled(resultSet.getBoolean("enabled"));

                String auth = resultSet.getString("authority");
                if (auth != null) authorities.add(new Authority(auth));

                String feed = resultSet.getString("feed");
                if (feed != null) feeds.add(new Feed(feed));

            }
        });

        if (Pattern.compile("\\@generalassemb\\.ly$").matcher(email).find()) {
            authorities.add(new Authority(AuthorityType.BASIC));
            user.setEnabled(true);
        }

        if (authorities.isEmpty()) return null;
        user.setAuthorities(authorities);
        user.setFeeds(feeds);
        return user;

    }



    @Override
    public UploadLog updateParserStatus(String filename, String parserStatus){

        jdbcTemplate.update(new PreparedStatementCreator() {
            @Override
            public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
                PreparedStatement ps = connection.prepareStatement(
                        "UPDATE logs.feed_upload_log SET parser_status=? WHERE filename = ?", RETURN_GENERATED_KEYS
                );
                int bindIndex = 1;
                ps.setString(bindIndex++, parserStatus);
                ps.setString(bindIndex++, filename);
                return ps;
            }
        });
        return getUploadLog(filename);

    }

    @Override
    public UploadLog getUploadLog(String filename){

        return jdbcTemplate.queryForObject("select l.uploadtype_id,l.email, l.upload_date, l.original_filename,l.filename, l.status as upload_status,\n" +
                        "fh.status as feedhandler_status, fh.message\n" +
                        "from logs.feed_upload_log l\n" +
                        "left join logs.feedhandler_log fh on fh.uploadtype_id = l.uploadtype_id and l.filename = fh.filename\n" +
                        "where filename = ?",
             new RowMapper<UploadLog>() {
            @Override
            public UploadLog mapRow(ResultSet resultSet, int i) throws SQLException {
                UploadLog out = new UploadLog();
                out.setUploadtypeID(resultSet.getLong("uploadtype_id"));
                out.setEmail(resultSet.getString("email"));
                out.setUploadDate(resultSet.getDate("upload_date"));
                out.setOriginalFilename(resultSet.getString("original_filename"));
                out.setFilename(resultSet.getString("filename"));
                out.setUploadStatus(resultSet.getString("upload_status"));
                out.setParserStatus(resultSet.getString("feedhandler_status"));
                out.setParserMessage(resultSet.getString("message"));
                return out;
            }
        }, filename);

    }

    @Override
    public SortedSet<UploadLog> getRecentUploadLogs(int count, int minutes, String user){

        String date_query = (minutes > 0)? " and l.upload_date > current_date - interval'" + minutes + " minutes' " : "";
        String limit_query = (count > 0)? " limit " + count : "";

        final String query =
        "select l.uploadtype_id,l.email, l.upload_date, l.original_filename, l.status as upload_status, fh.status as feedhandler_status, fh.message from logs.feed_upload_log l\n" +
        "join public.feed_def fd on fd.id = l.uploadtype_id\n" +
        "join feed_authority fa on fa.id_feed = fd.id\n" +
        "join users u on u.id = fa.id_user\n" +
        "left join logs.feedhandler_log fh on fh.uploadtype_id = l.uploadtype_id and l.filename = fh.filename\n" +
        "where u.email = '" + user + "'\n" +
        date_query + " order by l.upload_date desc " + limit_query;

        LOGGER.info(query);

        final SortedSet<UploadLog> out = new TreeSet<>(new UploadLogComparator());

        jdbcTemplate.query(new PreparedStatementCreator() {
            @Override
            public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
                return connection.prepareStatement(query);
            }
        }, new RowCallbackHandler() {
            @Override
            public void processRow(ResultSet resultSet) throws SQLException {
                UploadLog log = new UploadLog();
                log.setUploadtypeID(resultSet.getLong("uploadtype_id"));
                log.setEmail(resultSet.getString("email"));
                Timestamp timestamp = resultSet.getTimestamp("upload_date");

                Date date = new Date(timestamp.getTime());
                log.setUploadDate(date);
                log.setOriginalFilename(resultSet.getString("original_filename"));
                log.setUploadStatus(resultSet.getString("upload_status"));
                log.setParserStatus(resultSet.getString("feedhandler_status"));
                log.setParserMessage(resultSet.getString("message"));
                out.add(log);
            }
        });
        return out;
    }

    @Override
    public void logFileUpload(UploadLog log){

        final String queryInsert = "INSERT INTO logs.feed_upload_log (uploadtype_id, email, upload_date, original_filename, status, parser_status, filename) VALUES (?,?,?,?,?,?,?)";



        jdbcTemplate.update(new PreparedStatementCreator() {
            @Override
            public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
                PreparedStatement ps = connection.prepareStatement(queryInsert);
                ps.setLong(1,log.getUploadtypeID());
                ps.setString(2,log.getEmail());
                ps.setTimestamp(3,Timestamp.from(log.getUploadDate().toInstant()));
                ps.setString(4, log.getOriginalFilename());
                ps.setString(5, log.getUploadStatus());
                ps.setString(6,"");
                ps.setString(7, log.getFilename());
                return ps;
            }
        });

    }

    @Override
    public SortedSet<Link> getFrontPageLinks() {
        final String query = "select * from public.biportal_links";

        final SortedSet<Link> out = new TreeSet<>(new LinkComparator());

        jdbcTemplate.query(new PreparedStatementCreator() {
            @Override
            public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
                return connection.prepareStatement(query);
            }
        }, new RowCallbackHandler() {
            @Override
            public void processRow(ResultSet resultSet) throws SQLException {
                Link a = new Link();
                a.setOrder(resultSet.getLong("link_order"));
                a.setTitle(resultSet.getString("title"));
                a.setLink(resultSet.getString("link"));
                a.setDescription(resultSet.getString("description"));
                out.add(a);
            }
        });
        return out;
    }


}
