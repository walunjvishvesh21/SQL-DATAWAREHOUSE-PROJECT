
use master ;
go

----- drop and recreate 'DataWareHouse' -------

if exists (select 1 from sys.databases where name ='DataWareHouse')
begin 
     alter database DataWareHouse set single_user with rollback immediate;
	 drop Database DataWareHouse;

end ;

go

create database DataWareHouse;

go

create schema bronze;
go

create schema silver;

go

create schema gold;

go

------- create ddl scripts for  bronze all csv-------

if OBJECT_ID('bronze.crm_cust_info' ,'U') is not null
      drop table bronze.crm_cust_info;


create table bronze.crm_cust_info(
cst_id int,
cst_key  nvarchar(50),
cst_firstname nvarchar(50),
cst_lastname nvarchar(50),
cst_material_status nvarchar(50),
cst_gndr nvarchar(50),
cst_create_date date
);

if OBJECT_ID('bronze.crm_prd_info' ,'U') is not null
      drop table bronze.crm_prd_info;


create table bronze.crm_prd_info(
prd_id int,
prd_key  nvarchar(50),
prd_nm nvarchar(50),
prd_cost int,
prd_line nvarchar(50),
prd_start_dt datetime,
prd_end_dt datetime
);

if OBJECT_ID('bronze.crm_sales_details' ,'U') is not null
      drop table bronze.crm_sales_details;

create table bronze.crm_sales_details(
sls_ord_num nvarchar(50),
sls_prd_key  nvarchar(50),
sls_cust_id int ,
sls_order_dt int,
sls_ship_dt  int,
sls_due_dt int,
sls_sales  int,
sls_quantity int,
sls_price float 
);

if OBJECT_ID('bronze.erp_loc_a101' ,'U') is not null
      drop table bronze.erp_loc_a101;
create table bronze.erp_loc_a101(
cid nvarchar(50),
cntry nvarchar(50)
);
if OBJECT_ID('bronze.erp_cust_az12' ,'U') is not null
      drop table bronze.erp_cust_az12;
create table bronze.erp_cust_az12(
cid nvarchar(50),
bdate date,
gen nvarchar(50)
);

if OBJECT_ID('bronze.erp_px_cat_g1v2' ,'U') is not null
      drop table bronze.erp_px_cat_g1v2;

create table bronze.erp_px_cat_g1v2(
id nvarchar(50),
cat nvarchar(50),
subcat nvarchar(50),
maintenance nvarchar(50)
);

------creating ddl script for silver -------


if OBJECT_ID('silver.crm_cust_info' ,'U') is not null
      drop table silver.crm_cust_info;


create table silver.crm_cust_info(
cst_id int,
cst_key  nvarchar(50),
cst_firstname nvarchar(50),
cst_lastname nvarchar(50),
cst_material_status nvarchar(50),
cst_gndr nvarchar(50),
cst_create_date date,
dwh_create_date datetime2 default getdate()
);

if OBJECT_ID('silver.crm_prd_info' ,'U') is not null
      drop table silver.crm_prd_info;


create table silver.crm_prd_info(
prd_id int,
cat_id nvarchar(50),
prd_key  nvarchar(50),
prd_nm nvarchar(50),
prd_cost int,
prd_line nvarchar(50),
prd_start_dt date,
prd_end_dt date,
dwh_create_date datetime2 default getdate()
);

if OBJECT_ID('silver.crm_sales_details' ,'U') is not null
      drop table silver.crm_sales_details;

create table silver.crm_sales_details(
sls_ord_num nvarchar(50),
sls_prd_key  nvarchar(50),
sls_cust_id int ,
sls_order_dt date,
sls_ship_dt  date,
sls_due_dt date,
sls_sales  int,
sls_quantity int,
sls_price float,
dwh_create_date datetime2 default getdate()
);

if OBJECT_ID('silver.erp_loc_a101' ,'U') is not null
      drop table silver.erp_loc_a101;
create table silver.erp_loc_a101(
cid nvarchar(50),
cntry nvarchar(50),
dwh_create_date datetime2 default getdate()
);
if OBJECT_ID('silver.erp_cust_az12' ,'U') is not null
      drop table silver.erp_cust_az12;
create table silver.erp_cust_az12(
cid nvarchar(50),
bdate date,
gen nvarchar(50),
dwh_create_date datetime2 default getdate()
);

if OBJECT_ID('silver.erp_px_cat_g1v2' ,'U') is not null
      drop table silver.erp_px_cat_g1v2;

create table silver.erp_px_cat_g1v2(
id nvarchar(50),
cat nvarchar(50),
subcat nvarchar(50),
maintenance nvarchar(50),
dwh_create_date datetime2 default getdate()
);








































----- truncate  and bulk insert bronze-------
go
create or alter procedure bronze.load_bronze as 
begin

declare @start_time datetime , @end_time datetime, @batch_start_time datetime , @batch_end_time datetime

begin try 
set @batch_start_time = getdate();

print '====================================='
print'loading bronze layer' 
print' ====================================='


print '====================================='
print ' loading crm tables'
print ' ====================================='

set @start_time = GETDATE();
print ' truncating table bronze.crm_cust_info'

truncate table bronze.crm_cust_info;

print ' inserting into table bronze.crm_cust_info'

bulk insert bronze.crm_cust_info
from 'C:\Users\Vishvesh\Music\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
with (
      firstrow=2,
	  fieldterminator=',',
	  tablock
);
set @end_time = GETDATE();
print ' load duration ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds' ;
print '--------------------------------------------------------'




set @start_time = GETDATE();
print ' truncating table bronze.crm_prd_info'

truncate table bronze.crm_prd_info;

print ' inserting into table bronze.crm_prd_info'

bulk insert bronze.crm_prd_info
from 'C:\Users\Vishvesh\Music\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
with (
      firstrow=2,
	  fieldterminator=',',
	  tablock
);
set @end_time = GETDATE();
print ' load duration ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds' ;
print '--------------------------------------------------------'



set @start_time = GETDATE();
print ' truncating table bronze.crm_sales_details'

truncate table bronze.crm_sales_details;

print ' inserting into table bronze.crm_sales_details'

bulk insert bronze.crm_sales_details
from 'C:\Users\Vishvesh\Music\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
with (
      firstrow=2,
	  fieldterminator=',',
	  tablock
);
set @end_time = GETDATE();
print ' load duration ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds' ;
print '--------------------------------------------------------'




print '====================================='
print ' loading erp tables'
print ' ====================================='



set @start_time = GETDATE();
print ' truncating table bronze.erp_loc_a101'
truncate table bronze.erp_loc_a101;
print ' inserting data into table bronze.erp_loc_a101'
bulk insert bronze.erp_loc_a101
from 'C:\Users\Vishvesh\Music\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
with (
      firstrow=2,
	  fieldterminator=',',
	  tablock
);
set @end_time = GETDATE();
print ' load duration ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds' ;
print '--------------------------------------------------------'



set @start_time = GETDATE();
print ' truncating table bronze.erp_cust_az12'

truncate table bronze.erp_cust_az12;

print ' inserting data into table bronze.erp_cust_az12'

bulk insert bronze.erp_cust_az12
from 'C:\Users\Vishvesh\Music\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
with (
      firstrow=2,
	  fieldterminator=',',
	  tablock
);
set @end_time = GETDATE();
print ' load duration ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds' ;
print '--------------------------------------------------------'



set @start_time = GETDATE();
print ' truncating table bronze.erp_px_cat_g1v2'

truncate table bronze.erp_px_cat_g1v2;

print ' inserting into table bronze.erp_px_cat_g1v2'

bulk insert bronze.erp_px_cat_g1v2
from 'C:\Users\Vishvesh\Music\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
with (
      firstrow=2,
	  fieldterminator=',',
	  tablock
);
set @end_time = GETDATE();
print ' load duration ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds' ;
print '--------------------------------------------------------'

set @batch_end_time =GETDATE();
print'=========================================================='
print ' loading bronze layer is completed'
print 'total load duration' + cast (datediff(second, @batch_start_time, @batch_end_time) as nvarchar) + 'seconds' ;
end try 
begin catch
print '========================================================='
print ' error occured during bronze layer'
print 'error message' + error_message()
print ' error message ' + cast(error_number() as nvarchar)
print ' error message ' + cast (error_state() as nvarchar)
print '========================================================='

end catch 
end 




SELECT * from silver.crm_cust_info

select * from silver.erp_px_cat_g1v2