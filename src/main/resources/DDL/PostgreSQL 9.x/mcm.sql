


-------------------------------------------------

create table biportal_links
(
	id serial not null
		constraint biportal_links_pkey
			primary key,
	link_order integer not null,
	title varchar(100) not null,
	link varchar(200) not null,
	description varchar(200)
)
;

create unique index biportal_links_id_uindex
	on biportal_links (id)
;

create unique index biportal_links_link_order_uindex
	on biportal_links (link_order)
;

create unique index biportal_links_title_uindex
	on biportal_links (title)
;

comment on table biportal_links is 'front page links'
;

insert into biportal_links
(link_order, title, link, description)
values(1,'Link 1', 'https://www.google.com', 'Link description 1')



create table feed_authority
(
	id integer not null,
	id_user integer not null,
	id_feed integer not null
)
;

create table feed_def
(
	id integer not null,
	name varchar(50),
	bucket varchar(50),
	prefix varchar(50),
	reject_prefix varchar(20),
	archive_prefix varchar(20),
	file_prefix varchar(50)
)
;


;


create table logs.feed_upload_log
(
	uploadtype_id integer not null,
	email varchar(100) not null,
	upload_date timestamp not null,
	original_filename varchar(100) not null,
	status varchar(100),
	parser_status varchar(100),
	filename varchar(100)
)
;





insert into feed_def values (1,'feed 1', 'test-bucket','in','bad','bak','file_prefix');
insert into feed_authority values (1,1,1);

CREATE TABLE public.users_authority
(
    id integer NOT NULL,
    id_user bigint,
    id_authority bigint,
    CONSTRAINT users_authority_pkey PRIMARY KEY (id)
)
WITH (
    OIDS = FALSE
);

CREATE TABLE public.authority
(
    id integer NOT NULL,
    name character varying(50) COLLATE pg_catalog."default",
    CONSTRAINT authority_pkey PRIMARY KEY (id)
)
WITH (
    OIDS = FALSE
);

CREATE TABLE public.users
(
    id integer NOT NULL,
    email character varying(50) COLLATE pg_catalog."default",
    enabled boolean,
    CONSTRAINT users_pkey PRIMARY KEY (id),
    CONSTRAINT users_email_key UNIQUE (email)
)
WITH (
    OIDS = FALSE
);

insert into authority (id,name) values(1,'admin'), (2,'user');
insert into users (id, email, enabled) values (1,'test','true');
insert into users_authority (id, id_user, id_authority) values (1,1,1);

-- META --

CREATE SCHEMA meta;
SET search_path=meta,public;

CREATE TYPE meta.account_status AS ENUM
(
    'ACTIVE',
    'PAUSED',
    'EXPIRED',
    'DELETED'
);

CREATE TYPE meta.account_type AS ENUM
(
    'MSA',
    'BUSINESS_UNIT'
);

CREATE TYPE meta.campaign_status AS ENUM
(
    'ACTIVE',
    'PAUSED',
    'EXPIRED',
    'DELETED'
);

CREATE TYPE meta.campaign_type AS ENUM
(
    'CPA',
    'CPE',
    'CPC',
    'CPM',
    'CPP',
    'DCPM',
    'DCPC'
);

CREATE TYPE meta.marketplace_status AS ENUM
(
    'ACTIVE',
    'PAUSED',
    'EXPIRED',
    'TERMINATED',
    'MERGED',
    'DELETED'
);

CREATE TYPE meta.product_status AS ENUM
(
    'ACTIVE',
    'PAUSED',
    'EXPIRED',
    'MERGED',
    'DELETED'
);

CREATE TYPE meta.tag_type AS ENUM
(
    'CAMPAIGN',
    'BUSINESS_UNIT',
    'PRODUCT',
    'MARKETPLACE',
    'GEO'
);


CREATE TABLE meta.business_unit
(
  id serial NOT NULL,
  name character varying(100) NOT NULL,
  description text,
  status meta.account_status NOT NULL DEFAULT 'ACTIVE'::meta.account_status,
  status_updated timestamp without time zone NOT NULL DEFAULT now(),
  CONSTRAINT business_unit_pkey PRIMARY KEY (id),
  CONSTRAINT business_unit_name_key UNIQUE (name)
)
WITH (
  OIDS=FALSE
);

CREATE TABLE meta.marketplace
(
  id serial NOT NULL,
  name character varying(100),
  description text,
  status meta.marketplace_status NOT NULL,
  status_updated timestamp without time zone NOT NULL DEFAULT now(),
  contact_email character varying(100),
  contact_name character varying(100),
  CONSTRAINT marketplace_pkey PRIMARY KEY (id),
  CONSTRAINT marketplace_name_key UNIQUE (name)
)
WITH (
  OIDS=FALSE
);

CREATE TABLE meta.product
(
  code character varying(6) NOT NULL,
  name character varying(100) NOT NULL,
  description text,
  status meta.product_status NOT NULL,
  status_updated timestamp without time zone NOT NULL DEFAULT now(),
  CONSTRAINT product_pkey PRIMARY KEY (code),
  CONSTRAINT product_name_key UNIQUE (name)
)
WITH (
  OIDS=FALSE
);



CREATE TABLE meta.campaign
(
  product character varying(6) NOT NULL,
  tracker character varying(13) NOT NULL,
  type meta.campaign_type NOT NULL,
  marketplace integer NOT NULL,
  name character varying(100) NOT NULL,
  description text,
  status meta.campaign_status NOT NULL DEFAULT 'PAUSED'::meta.campaign_status,
  status_updated timestamp without time zone NOT NULL DEFAULT now(),
  cost_cents integer,
  CONSTRAINT campaign_pkey PRIMARY KEY (product, tracker),
  CONSTRAINT campaign_product_code_fkey FOREIGN KEY (product)
      REFERENCES meta.product (code) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION,
  CONSTRAINT campaign_tracker_key UNIQUE (tracker),
  CONSTRAINT campaign_tracker_check CHECK (tracker::text ~~ (product::text || '^%'::text)),
  CONSTRAINT cost_cents_gt_zero CHECK (cost_cents > 0 OR cost_cents IS NULL)
)
WITH (
  OIDS=FALSE
);

CREATE TABLE meta.tag_group
(
  id SERIAL PRIMARY KEY NOT NULL,
  is_mutex BOOL NOT NULL,
  applicable_to meta.tag_type[]
) WITH (
OIDS=FALSE
);


CREATE TABLE meta.tag
(
  id SERIAL PRIMARY KEY NOT NULL,
  "group" INT NOT NULL,
  value VARCHAR(50),
  CONSTRAINT "tag_meta.tag_group_group_id_fk" FOREIGN KEY ("group") REFERENCES meta.tag_group (id)
)WITH (
OIDS=FALSE
);
-- TODO: Move into meta.tag table definition
CREATE UNIQUE INDEX tag_group_value_uindex ON meta.tag ("group", value);

CREATE TABLE meta.campaign_tags
(
  campaign VARCHAR(13) NOT NULL,
  tag INT NOT NULL,
  CONSTRAINT campaign_tags_campaign_tag_pk PRIMARY KEY (campaign, tag),
  CONSTRAINT campaign_tags_campaign_tracker_fk FOREIGN KEY (campaign) REFERENCES meta.campaign (tracker),
  CONSTRAINT campaign_tags_tag_id_fk FOREIGN KEY (tag) REFERENCES meta.tag (id)
) WITH (
OIDS=FALSE
);

CREATE TABLE meta.marketplace_tags
(
  marketplace INTEGER NOT NULL,
  tag INT NOT NULL,
  CONSTRAINT marketplace_tags_account_tag_pk PRIMARY KEY (marketplace, tag),
  CONSTRAINT marketplace_tags_marketplace_id_fk FOREIGN KEY (marketplace) REFERENCES meta.marketplace (id),
  CONSTRAINT marketplace_tags_tag_id_fk FOREIGN KEY (tag) REFERENCES meta.tag (id)
);

CREATE TABLE meta.account_tags
(
  account INTEGER NOT NULL,
  tag INTEGER NOT NULL,
  CONSTRAINT account_tags_account_tag_pk PRIMARY KEY (account, tag),
  CONSTRAINT account_tags_account_tracker_fk FOREIGN KEY (account) REFERENCES meta.business_unit (id),
  CONSTRAINT account_tags_tag_id_fk FOREIGN KEY (tag) REFERENCES meta.tag (id)
);

CREATE TABLE meta.product_tags
(
  product VARCHAR(6) NOT NULL,
  tag INT NOT NULL,
  CONSTRAINT product_tags_product_tag_pk PRIMARY KEY (product, tag),
  CONSTRAINT product_tags_product_code_fk FOREIGN KEY (product) REFERENCES meta.product (code),
  CONSTRAINT product_tags_tag_id_fk FOREIGN KEY (tag) REFERENCES meta.tag (id)
);




CREATE TABLE meta.metro
(
  id SERIAL PRIMARY KEY NOT NULL,
  name VARCHAR(50) NOT NULL,
  description TEXT,
  extended BOOL
);
CREATE UNIQUE INDEX metro_name_uindex ON meta.metro (name);

CREATE TABLE meta.campaign_metros
(
  tracker VARCHAR(13) NOT NULL,
  metro INT NOT NULL,
  CONSTRAINT campaign_metro_pk PRIMARY KEY (tracker, metro),
  CONSTRAINT campaign_metro_campaign_tracker_fk FOREIGN KEY (tracker) REFERENCES meta.campaign (tracker),
  CONSTRAINT campaign_metro_metro_id_fk FOREIGN KEY (metro) REFERENCES meta.metro (id)
);

-- LOGS --

CREATE SCHEMA logs;

CREATE TABLE logs.campaign_status_changelog
(
  campaign character varying(13) NOT NULL,
  effective_date timestamp without time zone NOT NULL,
  status meta.campaign_status NOT NULL,
  change_number serial not null,
  CONSTRAINT campaign_status_changelog_pkey PRIMARY KEY (campaign, effective_date, change_number)
)
WITH (
  OIDS=FALSE
);

CREATE TABLE logs.marketplace_status_changelog
(
  marketplace integer NOT NULL,
  effective_date timestamp without time zone NOT NULL,
  status meta.marketplace_status NOT NULL,
  change_number serial not null,
  CONSTRAINT marketplace_status_changelog_pkey PRIMARY KEY (marketplace, effective_date, change_number)
)
WITH (
  OIDS=FALSE
);

CREATE TABLE logs.product_status_changelog
(
  product character varying(6) NOT NULL,
  effective_date timestamp without time zone NOT NULL,
  status meta.product_status NOT NULL,
  change_number serial not null,
  CONSTRAINT product_status_changelog_pkey PRIMARY KEY (product, effective_date, change_number)
)
WITH (
  OIDS=FALSE
);

CREATE TABLE logs.account_status_changelog
(
  account integer NOT NULL,
  effective_date timestamp without time zone NOT NULL,
  status meta.account_status NOT NULL,
  change_number serial not null,
  CONSTRAINT account_status_changelog_pkey PRIMARY KEY (account, effective_date, change_number)
)
WITH (
  OIDS=FALSE
);


create table meta.account_campaigns
(
	business_unit integer not null,
	tracker varchar(13) not null,
	linked_date time without time zone NOT NULL DEFAULT now(),
	CONSTRAINT account_campaigns_pkey PRIMARY KEY (business_unit, tracker),
  CONSTRAINT account_campaigns_business_unit_fkey FOREIGN KEY (business_unit)
      REFERENCES meta.business_unit (id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION,
  CONSTRAINT account_campaigns_tracker_fkey FOREIGN KEY (tracker)
      REFERENCES meta.campaign (tracker) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION,
  CONSTRAINT account_campaigns_tracker_key UNIQUE (tracker)
)
WITH (
  OIDS=FALSE
);

	create table logs.campaign_account_relationship_changelog
(
	tracker varchar(13) not null,
	account integer not null,
	effective_date timestamp without time zone NOT NULL DEFAULT now(),
	change_number serial not null,
	constraint campaign_account_relationship_changelog_pkey
		primary key (tracker, effective_date, change_number)
)
WITH (
  OIDS=FALSE
);


-- STATS --

CREATE SCHEMA stats;

CREATE TABLE stats.acquisition_facts
(
  campaign character varying(13) NOT NULL,
  statement_date date NOT NULL,
  acquisitions integer NOT NULL DEFAULT 0,
  acquisitions_payable integer NOT NULL DEFAULT 0,
  acquisitions_paid integer NOT NULL DEFAULT 0,
  acquisitions_not_paid integer NOT NULL DEFAULT 0,
  spend double precision NOT NULL DEFAULT 0.0,

  CONSTRAINT acquisition_facts_pkey PRIMARY KEY (campaign, statement_date),
  CONSTRAINT acquisition_facts_product_code_fkey FOREIGN KEY (campaign)
      REFERENCES meta.campaign (tracker) MATCH SIMPLE
      ON UPDATE RESTRICT ON DELETE RESTRICT

)
WITH (
  OIDS=FALSE
);



CREATE TABLE stats.acquisition_guidance
(

  campaign character varying(13) NOT NULL,
  effective_date timestamp without time zone NOT NULL,
  life_time_value_estimate double precision NOT NULL,

  CONSTRAINT acquisition_guidance_pkey PRIMARY KEY (campaign, effective_date),
  CONSTRAINT acquisition_guidance_product_code_fkey FOREIGN KEY (campaign)
      REFERENCES meta.campaign (tracker) MATCH SIMPLE
      ON UPDATE RESTRICT ON DELETE RESTRICT

)
WITH (
  OIDS=FALSE
);




-- Static Data
INSERT INTO meta.tag_group (id, is_mutex, applicable_to) VALUES (1, true,  '{MARKETPLACE,CAMPAIGN}');
INSERT INTO meta.tag_group (id, is_mutex, applicable_to) VALUES (2, true,  '{PRODUCT,MARKETPLACE,CAMPAIGN}');
INSERT INTO meta.tag_group (id, is_mutex, applicable_to) VALUES (3, true,  '{PRODUCT,MARKETPLACE,CAMPAIGN}');
INSERT INTO meta.tag_group (id, is_mutex, applicable_to) VALUES (4, true,  '{MARKETPLACE,CAMPAIGN}');
INSERT INTO meta.tag_group (id, is_mutex, applicable_to) VALUES (5, true,  '{MARKETPLACE,PRODUCT,CAMPAIGN}');
INSERT INTO meta.tag_group (id, is_mutex, applicable_to) VALUES (6, false, '{MARKETPLACE,CAMPAIGN}');

INSERT INTO meta.tag (id, "group", value) VALUES (1, 1, 'Social');
INSERT INTO meta.tag (id, "group", value) VALUES (2, 1, 'Affiliate');

INSERT INTO meta.tag (id, "group", value) VALUES (3, 2, 'Brand');
INSERT INTO meta.tag (id, "group", value) VALUES (4, 2, 'Non-Brand');

INSERT INTO meta.tag (id, "group", value) VALUES (5, 3, 'Email');

INSERT INTO meta.tag (id, "group", value) VALUES (6, 6, 'Search');

INSERT INTO meta.tag (id, "group", value) VALUES (7, 4, 'GDN');
INSERT INTO meta.tag (id, "group", value) VALUES (8, 4, 'SEM');

INSERT INTO meta.tag (id, "group", value) VALUES (9, 5, 'Sonja Sierra');
INSERT INTO meta.tag (id, "group", value) VALUES (10, 5, 'Elizabeth Vollman');
INSERT INTO meta.tag (id, "group", value) VALUES (11, 5, 'Lindsay Fitzgerald');
INSERT INTO meta.tag (id, "group", value) VALUES (12, 5, 'Erin Delman');


INSERT INTO meta.metro (id, name, description) VALUES (1, 'Melbourne', 'Melbourne');
INSERT INTO meta.metro (id, name, description) VALUES (2, 'Sydney', 'Sydney');
INSERT INTO meta.metro (id, name, description) VALUES (3, 'Austin', 'Austin');
INSERT INTO meta.metro (id, name, description) VALUES (4, 'Atlanta', 'Atlanta');
INSERT INTO meta.metro (id, name, description) VALUES (5, 'London', 'London');
INSERT INTO meta.metro (id, name, description) VALUES (6, 'Boston', 'Boston');
INSERT INTO meta.metro (id, name, description) VALUES (7, 'Singapore', 'Singapore');
INSERT INTO meta.metro (id, name, description) VALUES (8, 'DC', 'DC');
INSERT INTO meta.metro (id, name, description) VALUES (9, 'San Francisco', 'San Francisco');
INSERT INTO meta.metro (id, name, description) VALUES (10, 'Denver', 'Denver');
INSERT INTO meta.metro (id, name, description) VALUES (11, 'Los Angeles', 'Los Angeles');
INSERT INTO meta.metro (id, name, description) VALUES (12, 'New York City', 'New York City');
INSERT INTO meta.metro (id, name, description) VALUES (13, 'Chicago', 'Chicago');
INSERT INTO meta.metro (id, name, description) VALUES (14, 'Hong-Kong', 'Hong-Kong');
INSERT INTO meta.metro (id, name, description) VALUES (15, 'Seattle', 'Seattle');
