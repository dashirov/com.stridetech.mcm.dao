package com.stridetech.mcm.dao;

import com.stridetech.mcm.model.enums.*;
import com.stridetech.mcm.model.logs.ChangeLog;
import com.stridetech.mcm.model.meta.*;
import com.stridetech.mcm.model.security.*;

import java.util.Date;
import java.util.SortedMap;
import java.util.SortedSet;

public interface MCMServiceDao {

    /**
     *  CRUD BusinesssUnit
     */

    Account createBusinessUnit(Account account);
    Account retrieveBusinessUnit(Long Id);
    Account updateBusinessUnit(Account account);
    Account deleteBusinessUnit(Account account);
    ChangeLog<AccountStatus> getAccountStatusChangelog(Account account);
    SortedSet<Account> listBusinessUnits();
    SortedSet<Account> listBusinessUnitsInRetrospect(Date effectiveDate);
    SortedSet<Campaign> listBusinessUnitCampaigns(Account account);
    SortedSet<Campaign> listBusinessUnitCampaigns(Long accountId);

    /**
     *
     * CRUD Marketplace
     */

    Marketplace createMarketplace(Marketplace marketplace);
    Marketplace retrieveMarketplace(Long Id);
    Marketplace updateMarketplace(Marketplace marketplace);
    Marketplace deleteMarketplace(Marketplace marketplace);
    ChangeLog<MarketplaceStatus> getMarketplaceStatusChangelog(Marketplace marketplace);
    SortedSet<Marketplace> listMarketplaces();
    SortedSet<Marketplace> listMarketplacesInRetrospect(Date effectiveDate);

    /**
     * CRUD Product
     */

    Product createProduct(Product product);
    Product retrieveProduct(String code);
    Product updateProduct(Product product);
    Product deleteProduct(Product product);
    ChangeLog<ProductStatus> getProductStatusChangelog(Product product);
    SortedSet<Product> listProducts();
    SortedSet<Product> listProductsInRetrospect(Date effectiveDate);


    /**
     * CRUD Campaign
     */

    Campaign createCampaign(Campaign campaign);
    Campaign retrieveCampaign(String tracker);
    Campaign updateCampaign(Campaign campaign);
    Campaign deleteCampaign(Campaign campaign);
    ChangeLog<CampaignStatus> getCampaignStatusChangelog(Campaign campaign);
    ChangeLog<CampaignStatus> getCampaignStatusChangelog(String campaignTracker);
    SortedSet<Campaign> listCampaigns();
    SortedSet<Campaign> listCampaignsInRetrospect(Date effectiveDate);
    SortedSet<Campaign> listMarketplaceCampaigns(Marketplace marketplace);
    SortedSet<Campaign> listMarketplaceCampaignsInRetrospect(Marketplace marketplace, Date effectiveDate);
    SortedSet<Campaign> listMarketplaceCampaignsInRetrospect(Long marketplaceId, Date effectiveDate);
    SortedSet<Campaign> listProductCampaigns(Product product);
    SortedSet<Campaign> listProductCampaigns(String productCode);
    SortedSet<Campaign> listProductCampaignsInRetrospect(Product product, Date effectiveDate);


    /**
     * Unique object ( for update or insertion )
     * not myself, same name or id or whatever else must be unique
     */

    Boolean isNewMarketplaceUnique(Marketplace marketplace, Boolean doExcludeOwnId);
    Boolean isNewProductUnique(Product product, Boolean doExcludeOwnId);
    Boolean isNewCampaignUnique(Campaign campaign, Boolean doExcludeOwnId);
    Boolean isNewAccountUnique(Account account, Boolean doExcludeOwnId);

    TagGroup createTagGroup(TagGroup tagGroup);
    TagGroup retrieveTagGroup(Long id);
    TagGroup updateTagGroup(TagGroup tagGroup);
    TagGroup deleteTagGroup(TagGroup tagGroup);
    SortedSet<TagGroup> allApplicable(TagType type);
    SortedSet<Tag> retrieveTagGroupTags(Long tagGroupId);
    SortedSet<Tag> retrieveTagGroupTags(TagGroup tagGroup);
    Tag createTag(Tag tag);
    Tag retrieveTag(Long id);
    Tag updateTag(Tag tag);
    Tag deleteTag(Tag tag);

    SortedMap<Tag, SortedSet<Tag>> listCampaignTags(Campaign campaign);
    SortedMap<Tag, SortedSet<Tag>> listCampaignTags(String tracker);

    SortedMap<Tag, SortedSet<Tag>> listProductTags(Product product);
    SortedMap<Tag, SortedSet<Tag>> listProductTags(String code);
    SortedMap<Tag, SortedSet<Tag>> listAccountTags(Account account);
    SortedMap<Tag, SortedSet<Tag>> listAccountTags(Long id);
    SortedMap<Tag, SortedSet<Tag>> listMarketplaceTags(Marketplace marketplace);
    SortedMap<Tag, SortedSet<Tag>> listMarketplaceTags(Long id);
    SortedMap<Tag, SortedSet<Tag>> tagCampaign(Campaign campaign, Tag tag);
    SortedMap<Tag, SortedSet<Tag>> untagCampaign(Campaign campaign, Tag tag);
    SortedMap<Tag, SortedSet<Tag>> tagAccount(Account account, Tag tag);
    SortedMap<Tag, SortedSet<Tag>> untagAccount(Account account, Tag tag);
    SortedMap<Tag, SortedSet<Tag>> tagProduct(Product product, Tag tag);
    SortedMap<Tag, SortedSet<Tag>> untagProduct(Product product, Tag tag);
    SortedMap<Tag, SortedSet<Tag>> tagMarketplace(Marketplace marketplace, Tag tag);
    SortedMap<Tag, SortedSet<Tag>> untagMarketplace(Marketplace marketplace, Tag tag);

    SortedSet<TagGroup> listTagGroups();

    /**
     * Metros
     */

    SortedSet<Metro> listMetros();
    Metro retrieveMetro(Long id);
    Metro setCampaignMetro(Campaign campaign, Metro metro);
    Metro unsetCampaignMetro(Campaign campaign, Metro metro);
    SortedSet<Metro> listCampaignMetros(Campaign campaign);


    /**
     * Security
     */

    User getUserByEmail(String email);

    /***
     *  Feed Upload
     */

    SortedSet<Feed> listFeedDef(String user, boolean summary);
    Feed getFeed(Long fileType);
    void logFileUpload(UploadLog log);
    UploadLog updateParserStatus(String filename, String parserStatus);
    UploadLog getUploadLog(String filename);
    SortedSet<UploadLog> getRecentUploadLogs(int count, int minutes, String user);
    SortedSet<Link> getFrontPageLinks();
}


