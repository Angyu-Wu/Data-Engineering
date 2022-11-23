-- Databricks notebook source
select
  *
from
  ods_customer

-- COMMAND ----------

select
  *
from
  ods_prod_cat_info

-- COMMAND ----------

select
  *
from
  ods_transactions

-- COMMAND ----------

-- Create the DWD layers
-- Create a dwd_customer table
create table if not exists dwd_customer (customer_id string, DOB string, Gender string)

-- COMMAND ----------

-- Remove city code, which is meaningless
insert into
  dwd_customer (
    select
      customer_id,
      DOB,
      Gender
    from
      ods_customer
  )

-- COMMAND ----------

select
  *
from
  dwd_customer

-- COMMAND ----------

-- Create a wide table that combines ods_prod_cat_info, ods_customer, and ods_transactions
create or replace table  dwd_transactions (
  transaction_id string,
  cust_id string,
  tran_date string,
  prod_subcat_code string,
  prod_subcat string,
  prod_cat_code string,
  prod_cat string,
  Qty string,
  Rate string,
  Tax string,
  total_amt string,
  Store_type string,
  DOB string,
  Gender string
)

-- COMMAND ----------

insert into
  dwd_transactions (
    select
      transaction_id,
      cust_id,
      tran_date,
      prod_subcat_code,
      prod_subcat,
      ods_transactions.prod_cat_code,
      prod_cat,
      Qty,
      Rate,
      Tax,
      total_amt,
      Store_type,
      DOB,
      Gender
    from
      ods_transactions
      left join ods_prod_cat_info on ods_transactions.prod_subcat_code = ods_prod_cat_info.prod_sub_cat_code
      and ods_transactions.prod_cat_code = ods_prod_cat_info.prod_cat_code
      left join dwd_customer on ods_transactions.cust_id = dwd_customer.customer_id
  )

-- COMMAND ----------

select
  *
from
  dwd_transactions

-- COMMAND ----------

-- Create DWS layers
-- DWS layers by different topics
-- DWS table for product subcategory
create
or replace table dws_prod_subcat(
  transaction_id string,
  cust_id string,
  tran_date string,
  prod_subcat_code string,
  prod_subcat string
)

-- COMMAND ----------

insert into
  dws_prod_subcat (
    select
      transaction_id,
      cust_id,
      tran_date,
      prod_subcat_code,
      prod_subcat
    from
      dwd_transactions
  )

-- COMMAND ----------

select
  *
from
  dws_prod_subcat

-- COMMAND ----------

-- DWS table for product category
create
or replace table dws_prod_cat(
  transaction_id string,
  cust_id string,
  tran_date string,
  prod_cat_code string,
  prod_cat string
)

-- COMMAND ----------

insert into
  dws_prod_cat (
    select
      transaction_id,
      cust_id,
      tran_date,
      prod_cat_code,
      prod_cat
    from
      dwd_transactions
  )

-- COMMAND ----------

select
  *
from
  dws_prod_cat

-- COMMAND ----------

-- Create DWS for finance
create
or replace table dws_finance(
  transaction_id string,
  cust_id string,
  tran_date string,
  Qty string,
  Rate string,
  Tax string,
  total_amt string
)

-- COMMAND ----------

insert into
  dws_finance (
    select
      transaction_id,
      cust_id,
      tran_date,
      Qty,
      Rate,
      Tax,
      total_amt
    from
      dwd_transactions
  )

-- COMMAND ----------

select
  *
from
  dws_finance

-- COMMAND ----------

-- Create DWS for store type
create
or replace table dws_store_type(
  transaction_id string,
  cust_id string,
  tran_date string,
  Store_type string
)

-- COMMAND ----------

insert into
  dws_store_type (
    select
      transaction_id,
      cust_id,
      tran_date,
      Store_type
    from
      dwd_transactions
  )

-- COMMAND ----------

select
  *
from
  dws_store_type

-- COMMAND ----------

-- Create DWS for Gender
create
or replace table dws_gender(
  transaction_id string,
  cust_id string,
  tran_date string,
  Gender string
)

-- COMMAND ----------

insert into
  dws_gender (
    select
      transaction_id,
      cust_id,
      tran_date,
      Gender
    from
      dwd_transactions
  )

-- COMMAND ----------

select
  *
from
  dws_gender

-- COMMAND ----------

-- Create DWS table by year
-- product subcategory in 2011
create
or replace table dws_prod_subcat_2011(
  transaction_id string,
  cust_id string,
  tran_date string,
  prod_subcat_code string,
  prod_subcat string
)

-- COMMAND ----------

insert into
  dws_prod_subcat_2011 (
    select
      transaction_id,
      cust_id,
      tran_date,
      prod_subcat_code,
      prod_subcat
    from
      dws_prod_subcat
    where
      tran_date >= '2011-01-01'
      and tran_date < '2011-12-31'
  )

-- COMMAND ----------

select
  *
from
  dws_prod_subcat_2011

-- COMMAND ----------

-- product subcategory in 2012
create
or replace table dws_prod_subcat_2012(
  transaction_id string,
  cust_id string,
  tran_date string,
  prod_subcat_code string,
  prod_subcat string
)

-- COMMAND ----------

insert into
  dws_prod_subcat_2012 (
    select
      transaction_id,
      cust_id,
      tran_date,
      prod_subcat_code,
      prod_subcat
    from
      dws_prod_subcat
    where
      tran_date >= '2012-01-01'
      and tran_date < '2012-12-31'
  )

-- COMMAND ----------

select
  *
from
  dws_prod_subcat_2012

-- COMMAND ----------

-- product subcategory in 2013
create
or replace table dws_prod_subcat_2013(
  transaction_id string,
  cust_id string,
  tran_date string,
  prod_subcat_code string,
  prod_subcat string
)

-- COMMAND ----------

insert into
  dws_prod_subcat_2013 (
    select
      transaction_id,
      cust_id,
      tran_date,
      prod_subcat_code,
      prod_subcat
    from
      dws_prod_subcat
    where
      tran_date >= '2013-01-01'
      and tran_date < '2013-12-31'
  )

-- COMMAND ----------

select
  *
from
  dws_prod_subcat_2013

-- COMMAND ----------

-- product subcategory in 2014
create
or replace table dws_prod_subcat_2014(
  transaction_id string,
  cust_id string,
  tran_date string,
  prod_subcat_code string,
  prod_subcat string
)

-- COMMAND ----------

insert into
  dws_prod_subcat_2013 (
    select
      transaction_id,
      cust_id,
      tran_date,
      prod_subcat_code,
      prod_subcat
    from
      dws_prod_subcat
    where
      tran_date >= '2014-01-01'
      and tran_date < '2014-12-31'
  )

-- COMMAND ----------

select
  *
from
  dws_prod_subcat_2014

-- COMMAND ----------

-- DWS table for product category in 2011
create
or replace table dws_prod_cat_2011(
  transaction_id string,
  cust_id string,
  tran_date string,
  prod_cat_code string,
  prod_cat string
)

-- COMMAND ----------

insert into
  dws_prod_cat_2011 (
    select
      transaction_id,
      cust_id,
      tran_date,
      prod_cat_code,
      prod_cat
    from
      dws_prod_cat
    where
      tran_date >= '2011-01-01'
      and tran_date < '2011-12-31'
  )

-- COMMAND ----------

select
  *
from
  dws_prod_cat_2011

-- COMMAND ----------

-- DWS table for product category in 2012
create
or replace table dws_prod_cat_2012(
  transaction_id string,
  cust_id string,
  tran_date string,
  prod_cat_code string,
  prod_cat string
)

-- COMMAND ----------

insert into
  dws_prod_cat_2012 (
    select
      transaction_id,
      cust_id,
      tran_date,
      prod_cat_code,
      prod_cat
    from
      dws_prod_cat
    where
      tran_date >= '2012-01-01'
      and tran_date < '2012-12-31'
  )

-- COMMAND ----------

select
  *
from
  dws_prod_cat_2012

-- COMMAND ----------

-- DWS table for product category in 2013
create
or replace table dws_prod_cat_2013(
  transaction_id string,
  cust_id string,
  tran_date string,
  prod_cat_code string,
  prod_cat string
)

-- COMMAND ----------

insert into
  dws_prod_cat_2013 (
    select
      transaction_id,
      cust_id,
      tran_date,
      prod_cat_code,
      prod_cat
    from
      dws_prod_cat
    where
      tran_date >= '2013-01-01'
      and tran_date < '2013-12-31'
  )

-- COMMAND ----------

select
  *
from
  dws_prod_cat_2013

-- COMMAND ----------

-- DWS table for product category in 2014
create
or replace table dws_prod_cat_2014(
  transaction_id string,
  cust_id string,
  tran_date string,
  prod_cat_code string,
  prod_cat string
)

-- COMMAND ----------

insert into
  dws_prod_cat_2014 (
    select
      transaction_id,
      cust_id,
      tran_date,
      prod_cat_code,
      prod_cat
    from
      dws_prod_cat
    where
      tran_date >= '2014-01-01'
      and tran_date < '2014-12-31'
  )

-- COMMAND ----------

select
  *
from
  dws_prod_cat_2014

-- COMMAND ----------

-- Create DWS for finance in 2011
create
or replace table dws_finance_2011(
  transaction_id string,
  cust_id string,
  tran_date string,
  Qty string,
  Rate string,
  Tax string,
  total_amt string
)

-- COMMAND ----------

insert into
  dws_finance_2011 (
    select
      transaction_id,
      cust_id,
      tran_date,
      Qty,
      Rate,
      Tax,
      total_amt
    from
      dws_finance
    where
      tran_date >= '2011-01-01'
      and tran_date < '2011-12-31'
  )

-- COMMAND ----------

select
  *
from
  dws_finance_2011

-- COMMAND ----------

-- Create DWS for finance in 2012
create
or replace table dws_finance_2012(
  transaction_id string,
  cust_id string,
  tran_date string,
  Qty string,
  Rate string,
  Tax string,
  total_amt string
)

-- COMMAND ----------

insert into
  dws_finance_2012 (
    select
      transaction_id,
      cust_id,
      tran_date,
      Qty,
      Rate,
      Tax,
      total_amt
    from
      dws_finance
    where
      tran_date >= '2012-01-01'
      and tran_date < '2012-12-31'
  )

-- COMMAND ----------

select
  *
from
  dws_finance_2012

-- COMMAND ----------

-- Create DWS for finance in 2013
create
or replace table dws_finance_2013(
  transaction_id string,
  cust_id string,
  tran_date string,
  Qty string,
  Rate string,
  Tax string,
  total_amt string
)

-- COMMAND ----------

insert into
  dws_finance_2013 (
    select
      transaction_id,
      cust_id,
      tran_date,
      Qty,
      Rate,
      Tax,
      total_amt
    from
      dws_finance
    where
      tran_date >= '2013-01-01'
      and tran_date < '2013-12-31'
  )

-- COMMAND ----------

select
  *
from
  dws_finance_2013

-- COMMAND ----------

-- Create DWS for finance in 2014
create
or replace table dws_finance_2014(
  transaction_id string,
  cust_id string,
  tran_date string,
  Qty string,
  Rate string,
  Tax string,
  total_amt string
)

-- COMMAND ----------

insert into
  dws_finance_2014 (
    select
      transaction_id,
      cust_id,
      tran_date,
      Qty,
      Rate,
      Tax,
      total_amt
    from
      dws_finance
    where
      tran_date >= '2014-01-01'
      and tran_date < '2014-12-31'
  )

-- COMMAND ----------

select
  *
from
  dws_finance_2014

-- COMMAND ----------

-- Create DWS for store type in 2011
create
or replace table dws_store_type_2011(
  transaction_id string,
  cust_id string,
  tran_date string,
  Store_type string
)

-- COMMAND ----------

insert into
  dws_store_type_2011 (
    select
      transaction_id,
      cust_id,
      tran_date,
      Store_type
    from
      dws_store_type
    where
      tran_date >= '2011-01-01'
      and tran_date < '2011-12-31'
  )

-- COMMAND ----------

select
  *
from
  dws_store_type_2011

-- COMMAND ----------

-- Create DWS for store type in 2012
create
or replace table dws_store_type_2012(
  transaction_id string,
  cust_id string,
  tran_date string,
  Store_type string
)

-- COMMAND ----------

insert into
  dws_store_type_2012 (
    select
      transaction_id,
      cust_id,
      tran_date,
      Store_type
    from
      dws_store_type
    where
      tran_date >= '2012-01-01'
      and tran_date < '2012-12-31'
  )

-- COMMAND ----------

select
  *
from
  dws_store_type_2012

-- COMMAND ----------

-- Create DWS for store type in 2013
create
or replace table dws_store_type_2013(
  transaction_id string,
  cust_id string,
  tran_date string,
  Store_type string
)

-- COMMAND ----------

insert into
  dws_store_type_2013 (
    select
      transaction_id,
      cust_id,
      tran_date,
      Store_type
    from
      dws_store_type
    where
      tran_date >= '2013-01-01'
      and tran_date < '2013-12-31'
  )

-- COMMAND ----------

select
  *
from
  dws_store_type_2013

-- COMMAND ----------

-- Create DWS for store type in 2014
create
or replace table dws_store_type_2014(
  transaction_id string,
  cust_id string,
  tran_date string,
  Store_type string
)

-- COMMAND ----------

insert into
  dws_store_type_2014 (
    select
      transaction_id,
      cust_id,
      tran_date,
      Store_type
    from
      dws_store_type
    where
      tran_date >= '2014-01-01'
      and tran_date < '2014-12-31'
  )

-- COMMAND ----------

select
  *
from
  dws_store_type_2014

-- COMMAND ----------

-- Create DWS for Gender in 2011
create
or replace table dws_gender_2011(
  transaction_id string,
  cust_id string,
  tran_date string,
  Gender string
)

-- COMMAND ----------

insert into
  dws_gender_2011 (
    select
      transaction_id,
      cust_id,
      tran_date,
      Gender
    from
      dws_gender
    where
      tran_date >= '2011-01-01'
      and tran_date < '2011-12-31'
  )

-- COMMAND ----------

select
  *
from
  dws_gender_2011

-- COMMAND ----------

-- Create DWS for Gender in 2012
create
or replace table dws_gender_2012(
  transaction_id string,
  cust_id string,
  tran_date string,
  Gender string
)

-- COMMAND ----------

insert into
  dws_gender_2012 (
    select
      transaction_id,
      cust_id,
      tran_date,
      Gender
    from
      dws_gender
    where
      tran_date >= '2012-01-01'
      and tran_date < '2012-12-31'
  )

-- COMMAND ----------

select
  *
from
  dws_gender_2012

-- COMMAND ----------

-- Create DWS for Gender in 2013
create
or replace table dws_gender_2013(
  transaction_id string,
  cust_id string,
  tran_date string,
  Gender string
)

-- COMMAND ----------

insert into
  dws_gender_2013 (
    select
      transaction_id,
      cust_id,
      tran_date,
      Gender
    from
      dws_gender
    where
      tran_date >= '2013-01-01'
      and tran_date < '2013-12-31'
  )

-- COMMAND ----------

select
  *
from
  dws_gender_2013

-- COMMAND ----------

-- Create DWS for Gender in 2014
create
or replace table dws_gender_2014(
  transaction_id string,
  cust_id string,
  tran_date string,
  Gender string
)

-- COMMAND ----------

insert into
  dws_gender_2014 (
    select
      transaction_id,
      cust_id,
      tran_date,
      Gender
    from
      dws_gender
    where
      tran_date >= '2014-01-01'
      and tran_date < '2014-12-31'
  )

-- COMMAND ----------

select
  *
from
  dws_gender_2014

-- COMMAND ----------

-- ADS layers for Business Intelligence applications
-- Product category by gender by year
-- Count number of each store type by year
-- Total amount and total quanity by year
