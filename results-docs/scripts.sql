----Stage Table---------------------

create external table stage(
Time STRING,  
Electricity_Facility  Double,
Fans_Electricity  Double,
Cooling_Electricity  Double,
Heating_Electricity  Double,
InteriorLights_Electricity  Double,
InteriorEquipment_Electricity  Double,
Gas_Facility  Double,
Heating_Gas  Double,
InteriorEquipment_Gas  Double,
WaterHeater_Gas  Double)
PARTITIONED BY (place string)
row format delimited fields terminated BY ','
stored AS textfile
LOCATION '/user/cloudera/electricity_data'
tblproperties('skip.header.line.count'='1');


ALTER TABLE stage ADD PARTITION(place ='USA_AL_Anniston.Metro.AP.722287_TMY3')
LOCATION '/user/cloudera/electricity_data/USA_AL_Anniston.Metro.AP.722287_TMY3';

ALTER TABLE stage ADD PARTITION(place ='USA_AL_Auburn-Opelika.AP.722284_TMY3')
LOCATION '/user/cloudera/electricity_data/USA_AL_Auburn-Opelika.AP.722284_TMY3';

ALTER TABLE stage ADD PARTITION(place ='USA_AL_Birmingham.Muni.AP.722280_TMY3')
LOCATION '/user/cloudera/electricity_data/USA_AL_Birmingham.Muni.AP.722280_TMY3';


ALTER TABLE stage ADD PARTITION(place ='USA_AL_Dothan.Muni.AP.722268_TMY3')
LOCATION '/user/cloudera/electricity_data/USA_AL_Dothan.Muni.AP.722268_TMY3';

ALTER TABLE stage ADD PARTITION(place ='USA_AL_Fort.Rucker-Cairns.Field.722269_TMY3')
LOCATION '/user/cloudera/electricity_data/USA_AL_Fort.Rucker-Cairns.Field.722269_TMY3';

ALTER TABLE stage ADD PARTITION(place ='USA_AL_Gadsen.Muni.AWOS.722285_TMY3')
LOCATION '/user/cloudera/electricity_data/USA_AL_Gadsen.Muni.AWOS.722285_TMY3';


ALTER TABLE stage ADD PARTITION(place ='USA_AL_Huntsville.Intl.AP-Jones.Field.723230_TMY3')
LOCATION '/user/cloudera/electricity_data/USA_AL_Huntsville.Intl.AP-Jones.Field.723230_TMY3';

ALTER TABLE stage ADD PARTITION(place ='USA_AL_Maxwell.AFB.722265_TMY3')
LOCATION '/user/cloudera/electricity_data/USA_AL_Maxwell.AFB.722265_TMY3';

ALTER TABLE stage ADD PARTITION(place ='USA_AL_Mobile-Downtown.AP.722235_TMY3')
LOCATION '/user/cloudera/electricity_data/USA_AL_Mobile-Downtown.AP.722235_TMY3';


ALTER TABLE stage ADD PARTITION(place ='USA_AL_Mobile-Rgnl.AP.722230_TMY3')
LOCATION '/user/cloudera/electricity_data/USA_AL_Mobile-Rgnl.AP.722230_TMY3';

ALTER TABLE stage ADD PARTITION(place ='USA_AL_Montgomery-Dannelly.Field.722260_TMY3')
LOCATION '/user/cloudera/electricity_data/USA_AL_Montgomery-Dannelly.Field.722260_TMY3';

ALTER TABLE stage ADD PARTITION(place ='USA_AL_Muscle.Shoals.Rgnl.AP.723235_TMY3')
LOCATION '/user/cloudera/electricity_data/USA_AL_Muscle.Shoals.Rgnl.AP.723235_TMY3';

ALTER TABLE stage ADD PARTITION(place ='USA_AL_Troy.Air.Field.722267_TMY3')
LOCATION '/user/cloudera/electricity_data/USA_AL_Troy.Air.Field.722267_TMY3';

ALTER TABLE stage ADD PARTITION(place ='USA_AL_Tuscaloosa.Muni.AP.722286_TMY3')
LOCATION '/user/cloudera/electricity_data/USA_AL_Tuscaloosa.Muni.AP.722286_TMY3';


------Intermediat Views -----------------------

create  view view_1 as
with schema_1 as (
select  
cast(from_unixtime(unix_timestamp(concat("2004/",ltrim(time)), 'yyyy/MM/dd  HH:mm:ss')) as timestamp) as Time,
cast(split(time , "[/,:, ]")[1] as int) as month,
cast(split(time , "[/,:, ]")[2] as int) as day,
cast(split(time , "[/,:, ]")[4]as int) as hour,
split(split(INPUT__FILE__NAME , "[/]")[7], ".csv")[0] as building_name,
named_struct('electricity',Electricity_Facility, 'gas', Gas_Facility) as Facility_hourly,
named_struct('Fans',Fans_Electricity,'Cooling',Cooling_Electricity,'Heating',Heating_Electricity,'InteriorLights',InteriorLights_Electricity,'InteriorEquipment',InteriorEquipment_Electricity) as Electricity,
named_struct('Heating',Heating_Gas, 'InteriorEquipment',InteriorEquipment_Gas,'WaterHeater',WaterHeater_Gas) as Gas,
named_struct('electricity',cast(null as double), 'gas',cast(null as double)) as Facility_monthly
split(INPUT__FILE__NAME , "[/]")[6] as Place,
from stage)
select * from schema_1
where 
(building_name like 'RefBldgFullServiceRestaurantNew%'
 OR building_name like 'RefBldgHospitalNew%'
 OR building_name like 'RefBldgLargeHotelNew%'
 OR building_name like 'RefBldgOutPatientNew%'
 OR building_name like 'RefBldgPrimarySchoolNew%'
 OR building_name like 'RefBldgQuickServiceRestaurantNew%'
 OR building_name like 'RefBldgSecondarySchoolNew%'
 OR building_name like 'RefBldgSmallHotelNew%'
 OR building_name like 'RefBldgSuperMarketNew%'  )
;

create view view_2 as
with schema_2 as (
select  
cast(from_unixtime(unix_timestamp(concat("2004/",ltrim(time)), 'yyyy/MM/dd  HH:mm:ss')) as timestamp) as Time,
cast(split(time , "[/,:, ]")[1] as int) as month,
cast(split(time , "[/,:, ]")[2] as int) as day,
cast(split(time , "[/,:, ]")[4]as int) as hour,
split(split(INPUT__FILE__NAME , "[/]")[7], ".csv")[0] as building_name,
named_struct('electricity',Electricity_Facility, 'gas', Gas_Facility) as Facility_hourly,
named_struct('Fans',Fans_Electricity,'Cooling',Cooling_Electricity,'Heating',Heating_Electricity,'InteriorLights',InteriorLights_Electricity,'InteriorEquipment',InteriorEquipment_Electricity) as Electricity,
named_struct('Heating',Heating_Gas, 'InteriorEquipment',cast(null as double),'WaterHeater',InteriorEquipment_Gas) as Gas,
named_struct('electricity',WaterHeater_Gas, 'gas', cast(null as double)) as Facility_monthly
split(INPUT__FILE__NAME , "[/]")[6] as Place,
from stage)
select * from schema_2
where 
(building_name like 'RefBldgLargeOfficeNew%'
 OR building_name like 'RefBldgMediumOfficeNew%'
 OR building_name like 'RefBldgMidriseApartmentNew%'
 OR building_name like 'RefBldgSmallOfficeNew%' )
;


create view view_3 as
with schema_3 as (
select  
cast(from_unixtime(unix_timestamp(concat("2004/",ltrim(time)), 'yyyy/MM/dd  HH:mm:ss')) as timestamp) as Time,
cast(split(time , "[/,:, ]")[1] as int) as month,
cast(split(time , "[/,:, ]")[2] as int) as day,
cast(split(time , "[/,:, ]")[4]as int) as hour,
split(split(INPUT__FILE__NAME , "[/]")[7], ".csv")[0] as building_name,
named_struct('electricity',Electricity_Facility, 'gas', Gas_Facility) as Facility_hourly,
named_struct('Fans',Fans_Electricity,'Cooling',Cooling_Electricity,'Heating',Heating_Electricity,'InteriorLights',InteriorLights_Electricity,'InteriorEquipment',InteriorEquipment_Electricity) as Electricity,
named_struct('Heating',Heating_Gas, 'InteriorEquipment',cast(null as double),'WaterHeater',cast(null as double)) as Gas,
named_struct('electricity',InteriorEquipment_Gas, 'gas', WaterHeater_Gas) as Facility_monthly
split(INPUT__FILE__NAME , "[/]")[6] as Place, 
from stage)
select * from schema_3
where 
(building_name like 'RefBldgStand-aloneRetailNew%'
 OR building_name like 'RefBldgStripMallNew%'
 OR building_name like 'RefBldgWarehouseNew%' )
;

------------Final View-----------------

create view vw_energy_new as 
select * from view_1 union all
select * from view_2 union all
select * from view_3
;


-------------Structuring the data--------------------
create table managed_stage(
Time timestamp,
month int,
day int,
hour int,
building_name string,
facility_hourly struct<electricity:double,gas:double>,
electricity struct<fans:double,cooling:double,heating:double,interiorlights:double,interiorequipment:double>,
gas struct<heating:double,interiorequipment:double,waterheater:double>,
facility_monthly struct<electricity:double,gas:double>
)
PARTITIONED BY (place string)
row format delimited fields terminated BY ','
COLLECTION ITEMS TERMINATED BY '$'
stored AS textfile;


---------Loading into managed stage table-----------------
insert into table managed_stage partition(place)
select * from vw_energy_new


----energy Table---------------------

create external table  energy(
Time timestamp,
month int,
day int,
hour int,
building_name string,
facility_hourly struct<electricity:double,gas:double>,
electricity struct<fans:double,cooling:double,heating:double,interiorlights:double,interiorequipment:double>,
gas struct<heating:double,interiorequipment:double,waterheater:double>,
facility_monthly struct<electricity:double,gas:double>
)
PARTITIONED BY (place string)
row format delimited fields terminated BY ','
COLLECTION ITEMS TERMINATED BY '$'
stored AS textfile
LOCATION  '/user/cloudera/prod_prod_electricity_data'  ;

ALTER TABLE energy ADD PARTITION(place ='USA_AL_Anniston.Metro.AP.722287_TMY3')
LOCATION '/user/cloudera/prod_electricity_data/place=USA_AL_Anniston.Metro.AP.722287_TMY3';

ALTER TABLE energy ADD PARTITION(place ='USA_AL_Auburn-Opelika.AP.722284_TMY3')
LOCATION '/user/cloudera/prod_electricity_data/place=USA_AL_Auburn-Opelika.AP.722284_TMY3';

ALTER TABLE energy ADD PARTITION(place ='USA_AL_Birmingham.Muni.AP.722280_TMY3')
LOCATION '/user/cloudera/prod_electricity_data/place=USA_AL_Birmingham.Muni.AP.722280_TMY3';


ALTER TABLE energy ADD PARTITION(place ='USA_AL_Dothan.Muni.AP.722268_TMY3')
LOCATION '/user/cloudera/prod_electricity_data/place=USA_AL_Dothan.Muni.AP.722268_TMY3';

ALTER TABLE energy ADD PARTITION(place ='USA_AL_Fort.Rucker-Cairns.Field.722269_TMY3')
LOCATION '/user/cloudera/prod_electricity_data/place=USA_AL_Fort.Rucker-Cairns.Field.722269_TMY3';

ALTER TABLE energy ADD PARTITION(place ='USA_AL_Gadsen.Muni.AWOS.722285_TMY3')
LOCATION '/user/cloudera/prod_electricity_data/place=USA_AL_Gadsen.Muni.AWOS.722285_TMY3';


ALTER TABLE energy ADD PARTITION(place ='USA_AL_Huntsville.Intl.AP-Jones.Field.723230_TMY3')
LOCATION '/user/cloudera/prod_electricity_data/place=USA_AL_Huntsville.Intl.AP-Jones.Field.723230_TMY3';

ALTER TABLE energy ADD PARTITION(place ='USA_AL_Maxwell.AFB.722265_TMY3')
LOCATION '/user/cloudera/prod_electricity_data/place=USA_AL_Maxwell.AFB.722265_TMY3';

ALTER TABLE energy ADD PARTITION(place ='USA_AL_Mobile-Downtown.AP.722235_TMY3')
LOCATION '/user/cloudera/prod_electricity_data/place=USA_AL_Mobile-Downtown.AP.722235_TMY3';


ALTER TABLE energy ADD PARTITION(place ='USA_AL_Mobile-Rgnl.AP.722230_TMY3')
LOCATION '/user/cloudera/prod_electricity_data/place=USA_AL_Mobile-Rgnl.AP.722230_TMY3';

ALTER TABLE energy ADD PARTITION(place ='USA_AL_Montgomery-Dannelly.Field.722260_TMY3')
LOCATION '/user/cloudera/prod_electricity_data/place=USA_AL_Montgomery-Dannelly.Field.722260_TMY3';

ALTER TABLE energy ADD PARTITION(place ='USA_AL_Muscle.Shoals.Rgnl.AP.723235_TMY3')
LOCATION '/user/cloudera/prod_electricity_data/place=USA_AL_Muscle.Shoals.Rgnl.AP.723235_TMY3';

ALTER TABLE energy ADD PARTITION(place ='USA_AL_Troy.Air.Field.722267_TMY3')
LOCATION '/user/cloudera/prod_electricity_data/place=USA_AL_Troy.Air.Field.722267_TMY3';

ALTER TABLE energy ADD PARTITION(place ='USA_AL_Tuscaloosa.Muni.AP.722286_TMY3')
LOCATION '/user/cloudera/prod_electricity_data/place=USA_AL_Tuscaloosa.Muni.AP.722286_TMY3';

--------******Much better energy table ---> energy_v2------------------

create external table  energy_V2(
Time timestamp,
month int,
day int,
hour int,
building_name string,
facility_hourly struct<electricity:double,gas:double>,
electricity struct<fans:double,cooling:double,heating:double,interiorlights:double,interiorequipment:double>,
gas struct<heating:double,interiorequipment:double,waterheater:double>,
facility_monthly struct<electricity:double,gas:double>
)
PARTITIONED BY (place string)
CLUSTERED BY (month) INTO 12 BUCKETS
row format delimited fields terminated BY ','
COLLECTION ITEMS TERMINATED BY '$'
stored AS textfile
LOCATION  '/user/cloudera/prod_energy_V2'  ;


INSERT INTO energy_V2 PARTITION(place) SELECT * FROM energy;







