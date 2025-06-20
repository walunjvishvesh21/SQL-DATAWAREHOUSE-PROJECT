create or alter procedure bronze.load_bronze as 

begin
declare @start_time as datetime, @end_time as datetime, @batch_start_time as datetime, @batch_end_time as datetime;
begin try 
set @batch_start_time = GETDATE();
print('================================================');
print('loading bronze layer');
print('================================================');


print('================================================');
print('loading crm layer');
print('================================================');


set @start_time =getdate(); 
print('>>> truncating table : bronze.crm_cust_info');
truncate table bronze.crm_cust_info;
print('>>> inserting data into table : bronze.crm_cust_info');
bulk insert bronze.crm_cust_info
from 'C:\Users\Vishvesh\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
with (
firstrow=2,
fieldterminator=',',
tablock
);
set @end_time =getdate(); 
print '>>>> load duration : ' + cast(datediff(second, @start_time,@end_time) as nvarchar) + 'seconds';
print('==========================================')


set @start_time =getdate(); 
print('>>> truncating table : bronze.crm_prd_info');
truncate table bronze.crm_prd_info;
print('>>> inserting data into table : bronze.crm_prd_info');
bulk insert bronze.crm_prd_info
from 'C:\Users\Vishvesh\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
with (
firstrow=2,
fieldterminator=',',
tablock
);

set @end_time =getdate(); 
print '>>>> load duration : ' + cast(datediff(second, @start_time,@end_time) as nvarchar) + 'seconds';
print('==========================================');




set @start_time =getdate(); 
print('>>> truncating table : bronze.crm_sales_details');
truncate table bronze.crm_sales_details;
print('>>> inserting data into table : bronze.crm_sales_details');
bulk insert bronze.crm_sales_details
from 'C:\Users\Vishvesh\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
with (
firstrow=2,
fieldterminator=',',
tablock
);

set @end_time =getdate(); 
print '>>>> load duration : ' + cast(datediff(second, @start_time,@end_time) as nvarchar) + 'seconds';
print('==========================================');





print('================================================');
print('loading erp layer');
print('================================================');


set @start_time =getdate(); 


print('>>> truncating table : bronze.erp_cust_az12');
truncate table bronze.erp_cust_az12;
print('>>> inserting data into table : bronze.erp_cust_az12');
bulk insert bronze.erp_cust_az12
from 'C:\Users\Vishvesh\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
with (
firstrow=2,
fieldterminator=',',
tablock
);

set @end_time =getdate(); 
print '>>>> load duration : ' + cast(datediff(second, @start_time,@end_time) as nvarchar) + 'seconds';
print('==========================================');



set @start_time =getdate(); 


print('>>> truncating table : bronze.erp_loc_a101');
truncate table bronze.erp_loc_a101;
print('>>> inserting data into table : bronze.erp_loc_a101');
bulk insert bronze.erp_loc_a101
from 'C:\Users\Vishvesh\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
with (
firstrow=2,
fieldterminator=',',
tablock
);


set @end_time =getdate(); 
print '>>>> load duration : ' + cast(datediff(second, @start_time,@end_time) as nvarchar) + 'seconds';
print('==========================================');



set @start_time =getdate(); 


print('>>> truncating table : bronze.erp_px_cat_g1v2');
truncate table bronze.erp_px_cat_g1v2;
print('>>> inserting data into table : bronze.erp_px_cat_g1v2');
bulk insert bronze.erp_px_cat_g1v2
from 'C:\Users\Vishvesh\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
with (
firstrow=2,
fieldterminator=',',
tablock
);

set @end_time =getdate(); 
print '>>>> load duration : ' + cast(datediff(second, @start_time,@end_time) as nvarchar) + 'seconds';
print('==========================================');

set @batch_end_time= getdate();
print('============================================================');
print('loading bronze layer completed');
print('total duration' + cast(datediff(second,@batch_start_time,@batch_end_time) as nvarchar) + ' seconds' );
end try 
begin catch 
print ('=========================================================');
print ' error message '+ (error_message());
print ' error message '+ cast(error_number() as nvarchar);
print ' error message '+ cast(error_state() as nvarchar);
print('===============================================');
end catch 
end
