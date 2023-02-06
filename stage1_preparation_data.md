## Stage #1. Data Preparation

This step is optional. You can skip this step to “Step#1-1. Import Korea Pension Data from Analytic Hub”.

Go to Korea government public dataset portal. 
Search “National Pension Service”
Go to detail link.
Download pension datasets from 2021-Nov to 2022-Sep.
(You can find historical dataset from the below links)

(Optional) You can download these files from the below links

Date
Link
2022 Oct
Link
2022 Sep
Link
2022 Aug
Link
2022 Jul
Link
2022 Jun
Link
2022 May
Link
2022 Apr
Link
2022 Mar
Link
2022 Feb
Link
2022 Jan
Link
2021 Dec
Link
2021 Nov
Link


These dataset files have been encoded in CP949 character set. So you should convert these files into utf8 character set. The following commands can help step#5~6.


#!/bin/bash
#
# wget, iconv commands must be installed before to launch this script.
while read -r line; do
 link=`echo ${line}|awk '{print $1}'`
 file=`echo ${line}|awk '{print $2}'`
 wget -q -O - ${link} | iconv -f cp949 -t utf8 > ${file}
done <<EOF
https://www.data.go.kr/cmm/cmm/fileDownload.do?atchFileId=FILE_000000002642986&fileDetailSn=1 national_pension_202210.csv
https://www.data.go.kr/cmm/cmm/fileDownload.do?atchFileId=FILE_000000002628453&fileDetailSn=1 national_pension_202209.csv
https://www.data.go.kr/cmm/cmm/fileDownload.do?atchFileId=FILE_000000002609439&fileDetailSn=1 national_pension_202208.csv
https://www.data.go.kr/cmm/cmm/fileDownload.do?atchFileId=FILE_000000002590223&fileDetailSn=1 national_pension_202207.csv
https://www.data.go.kr/cmm/cmm/fileDownload.do?atchFileId=FILE_000000002572552&fileDetailSn=1 national_pension_202206.csv
https://www.data.go.kr/cmm/cmm/fileDownload.do?atchFileId=FILE_000000002556708&fileDetailSn=1 national_pension_202205.csv
https://www.data.go.kr/cmm/cmm/fileDownload.do?atchFileId=FILE_000000002536568&fileDetailSn=1 national_pension_202204.csv
https://www.data.go.kr/cmm/cmm/fileDownload.do?atchFileId=FILE_000000002527955&fileDetailSn=1 national_pension_202203.csv
https://www.data.go.kr/cmm/cmm/fileDownload.do?atchFileId=FILE_000000002518377&fileDetailSn=1 national_pension_202202.csv
https://www.data.go.kr/cmm/cmm/fileDownload.do?atchFileId=FILE_000000002512856&fileDetailSn=1 national_pension_202201.csv
https://www.data.go.kr/cmm/cmm/fileDownload.do?atchFileId=FILE_000000002505510&fileDetailSn=1 national_pension_202212.csv
https://www.data.go.kr/cmm/cmm/fileDownload.do?atchFileId=FILE_000000002494556&fileDetailSn=1 national_pension_202111.csv
https://www.data.go.kr/cmm/cmm/fileDownload.do?atchFileId=FILE_000000002484364&fileDetailSn=1 national_pension_202110.csv
EOF
 




Go to “BigQuery workspace” UI and execute the following DDL. This script will make a “sample_ds” dataset and “national_penstion_raw” table in this dataset. Before executing it, you need to select the appropriate project and location.

[Project Selection]


[Region selection]


create schema `sample_ds`;

CREATE TABLE `sample_ds`.national_pension_raw (
	create_yearmonth STRING(10),
	biz_workplace_name STRING(250),
	biz_regid STRING(15),
	register_status INT64,
	postal_code STRING(10),
	address_townbase STRING(15),
	address_streetbase STRING(15),
	legal_address_code STRING(15),
	admin_address_code STRING(15),
	prov_code INT64,
	county_code INT64,
	town_code INT64,
	legal_personal_type INT64,
	biz_category_code STRING(15),
	biz_category STRING(250),
	applied_date DATETIME,
	reregistered_date DATETIME,
	expired_date DATETIME,
	num_of_members INT64,
	monthly_fixed_amount INT64,
	num_of_new_member INT64,
	num_of_lost_members INT64,
);

Upload the previous downloaded files into GCS.



Load these monthly pension files into BigQuery UI.


load data into `sample_ds`.`national_pension_raw`
from files (
  format = 'CSV',
  skip_leading_rows=1,
  uris = [
  'gs://<<gcs_bucket>>/upload/national_pension_202110.csv',
  'gs://<<gcs_bucket>>/upload/national_pension_202111.csv',
  'gs://<<gcs_bucket>>/upload/national_pension_202112.csv',
  'gs://<<gcs_bucket>>/upload/national_pension_202201.csv',
  'gs://<<gcs_bucket>>/upload/national_pension_202202.csv',
  'gs://<<gcs_bucket>>/upload/national_pension_202203.csv',
  'gs://<<gcs_bucket>>/upload/national_pension_202204.csv',
  'gs://<<gcs_bucket>>/upload/national_pension_202205.csv',
  'gs://<<gcs_bucket>>/upload/national_pension_202206.csv',
  'gs://<<gcs_bucket>>/upload/national_pension_202207.csv',
  'gs://<<gcs_bucket>>/upload/national_pension_202208.csv',
  'gs://<<gcs_bucket>>/upload/national_pension_202209.csv',
  'gs://<<gcs_bucket>>/upload/national_pension_202210.csv'
  ]
)



Geolocation can be found in the public dataset - “Small Office Commercial Information”. “Small Office Commercial Information” holds information about Small/Medium-size corporation’s GeoLocation information. But it’s hard to calculate geolocation (longitude, latitude). So Alternatively, refined information from the public GitHub site will be used in this lab. Please execute the following commands in the shell.


#
wget https://github.com/kpyopark/looker_hol_script/raw/main/geolocation.csv
wget https://github.com/kpyopark/looker_hol_script/raw/main/bizcategory.csv

Upload them into GCS bucket.
Create the following table in BigQuery UI.


CREATE TABLE sample_ds.postal_location (
  postcode STRING(10),
  postcode_l1 STRING(10),
  postcode_l2 STRING(10),
  postcode_l3 STRING(10),
  longitude FLOAT64,
  latitude FLOAT64,
  longitude_l1 FLOAT64,
  latitude_l1 FLOAT64,
  longitude_l2 FLOAT64,
  latitude_l2 FLOAT64,
  longitude_l3 FLOAT64,
  latitude_l3 FLOAT64
);




Load “geolocation.csv” file into this table.


load data into sample_ds.postal_location
from files (
  format = 'CSV',
  skip_leading_rows=1,
  uris = ['gs://<<gcs bucket>>/geolocation.csv']
)
;





Create the following table in BigQuery UI.


CREATE TABLE sample_ds.bizcategory (
  cat_code STRING(10),
  cat_l1_code STRING(10),
  cat_l1 STRING(100),
  cat_l2_code STRING(10),
  cat_l2 STRING(100),
  cat_l3_code STRING(10),
  cat_l3 STRING(100),
  cat_l4_code STRING(10),
  cat_l4 STRING(100),
  cat_l5 STRING(100)
);

Load “bizcategory.csv” file into this table. 


load data into sample_ds.bizcategory
from files (
  format = 'CSV',
  skip_leading_rows=1,
  uris = ['gs://<<gcs bucket>>/bizcategory.csv']
)
;

OK. All necessary records are loaded on BigQuery.

