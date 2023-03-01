*************************************************
-----d1------------
dataset rr_response;

create table `idea-player-d-ba2a.rr_response.rating_and_reviews_blank_response` (
transaction_type string,
customer_request_identifier bigint,
customer_request_date timestamp,
business_partner_no bigint not null,
haal_export_timestamp timestamp,
request_details json);

insert into `idea-player-d-ba2a.rr_response.rating_and_reviews_blank_response`
(transaction_type, customer_request_identifier, customer_request_date, business_partner_no, haal_export_timestamp, request_details)
values
('APRatingsAndReviews',2016,'2022-11-11T06:20:03.000Z',1232243,'2022-11-14T12:43:29.441Z',json'[]'),
('APRatingsAndReviews',2016,'2022-11-11T06:20:03.000Z',1232243,'2022-11-14T12:43:29.441Z',json'[{"order_code":"26638761213","product_name":"BROOKE RW WIDE WCT JOGGER","size_name":"L","date_of_purchase":"2022-02-24 00:00:00","date_of_review":"2022-03-14 00:00:00","rating":"5","review_status":"Published"},{"order_code":"26591425713","product_name":"DIONNE coat","size_name":"L","date_of_purchase":"2022-03-14 00:00:00","date_of_review":"2022-03-24 00:00:00","rating":"","review_status":"Published"}]');


-----d2------------
dataset  sales_response;

create table `idea-player-d-ba2a.sales_response.online_sales_response` (
transaction_type string,
customer_request_identifier bigint,
customer_request_date timestamp,
business_partner_no bigint not null,
haal_export_timestamp timestamp,
request_details json);

insert into `idea-player-d-ba2a.sales_response.online_sales_response`
(transaction_type, customer_request_identifier, customer_request_date, business_partner_no, haal_export_timestamp, request_details)
values
('APCustomerOrders',89554,'2021-11-17T21:00:05.000Z',11111,'2022-10-23T11:10:26.945Z',json'[{"order_code":"31724932474","department_corporate_brand_name":"H&M","article_no":"0964595001","product_name":"Siri top","quantity":2,"currency":"EUR","order_item_price":12,"vat_amount":0,"freight_amount":0,"order_status":"Delivered/Returned","order_date":"2021-07-28 05:48:00"},{"order_code":"41187655964","department_corporate_brand_name":"H&M","article_no":"0998428002","product_name":"Sweater Sleeveless Mezzo","quantity":1,"currency":"EUR","order_item_price":8,"vat_amount":0,"freight_amount":0,"order_status":"Delivered/Returned","order_date":"2022-05-27 22:47:00"},{"order_code":"35446096814","department_corporate_brand_name":"H&M","article_no":"0971070017","product_name":"Shane fleece hood TP","quantity":1,"currency":"EUR","order_item_price":11,"vat_amount":0,"freight_amount":0,"order_status":"Dispatched","order_date":"2021-11-19 04:10:00"},{"order_code":"43159712734","department_corporate_brand_name":"Weekday","article_no":"1110443003","product_name":"Easy Cropped Tank Top 2 pack","quantity":1,"currency":"EUR","order_item_price":15,"vat_amount":0,"freight_amount":0,"order_status":"Dispatched","order_date":"2022-07-14 11:46:00"},{"order_code":"35732325264","department_corporate_brand_name":"H&M","article_no":"0944339006","product_name":"Lash SP bralette Ama","quantity":1,"currency":"EUR","order_item_price":15,"vat_amount":0,"freight_amount":0,"order_status":"Dispatched","order_date":"2021-11-28 09:56:00"},{"order_code":"35732325264","department_corporate_brand_name":"H&M","article_no":"0971070005","product_name":"Shane fleece hood TP","quantity":1,"currency":"EUR","order_item_price":11,"vat_amount":0,"freight_amount":0,"order_status":"Delivered/Returned","order_date":"2021-12-08 23:45:00"},{"order_code":"40830742024","department_corporate_brand_name":"Monki","article_no":"0953942007","product_name":"Cilla trousers","quantity":1,"currency":"EUR","order_item_price":19,"vat_amount":0,"freight_amount":0,"order_status":"Delivered/Returned","order_date":"2022-05-23 07:45:00"},{"order_code":"42145308944","department_corporate_brand_name":"H&M","article_no":"0975939004","product_name":"Merry Trash HW mom dnm trs","quantity":1,"currency":"EUR","order_item_price":17,"vat_amount":0,"freight_amount":0,"order_status":"Delivered/Returned","order_date":"2022-06-29 17:46:00"},{"order_code":"42145308944","department_corporate_brand_name":"H&M","article_no":"1045398007","product_name":"Aiko sweater","quantity":1,"currency":"EUR","order_item_price":4,"vat_amount":0,"freight_amount":0,"order_status":"Dispatched","order_date":"2022-06-20 04:07:00"}]'),
('APCustomerOrders',89555,'2021-11-17T21:00:05.000Z',11112,'2022-10-23T11:10:26.945Z',json'[{"order_code":"31724932475","department_corporate_brand_name":"H&M","article_no":"0964595002","product_name":"Siri top","quantity":2,"currency":"EUR","order_item_price":12,"vat_amount":0,"freight_amount":0,"order_status":"Delivered/Returned","order_date":"2021-07-28 05:48:00"},{"order_code":"41187655965","department_corporate_brand_name":"H&M","article_no":"0998428002","product_name":"Sweater Sleeveless Mezzo","quantity":1,"currency":"EUR","order_item_price":8,"vat_amount":0,"freight_amount":0,"order_status":"Delivered/Returned","order_date":"2022-05-27 22:47:00"},{"order_code":"35446096815","department_corporate_brand_name":"H&M","article_no":"0971070018","product_name":"Shane fleece hood TP","quantity":1,"currency":"EUR","order_item_price":11,"vat_amount":0,"freight_amount":0,"order_status":"Dispatched","order_date":"2021-11-19 04:10:00"},{"order_code":"43159712735","department_corporate_brand_name":"Weekday","article_no":"1110443004","product_name":"Easy Cropped Tank Top 2 pack","quantity":1,"currency":"EUR","order_item_price":15,"vat_amount":0,"freight_amount":0,"order_status":"Dispatched","order_date":"2022-07-14 11:46:00"},{"order_code":"35732325265","department_corporate_brand_name":"H&M","article_no":"0944339007","product_name":"Lash SP bralette Ama","quantity":1,"currency":"EUR","order_item_price":15,"vat_amount":0,"freight_amount":0,"order_status":"Dispatched","order_date":"2021-11-28 09:56:00"},{"order_code":"35732325264","department_corporate_brand_name":"H&M","article_no":"0971070005","product_name":"Shane fleece hood TP","quantity":1,"currency":"EUR","order_item_price":11,"vat_amount":0,"freight_amount":0,"order_status":"Delivered/Returned","order_date":"2021-12-08 23:45:00"},{"order_code":"40830742025","department_corporate_brand_name":"Monki","article_no":"0953942008","product_name":"Cilla trousers","quantity":1,"currency":"EUR","order_item_price":19,"vat_amount":0,"freight_amount":0,"order_status":"Delivered/Returned","order_date":"2022-05-23 07:45:00"},{"order_code":"42145308945","department_corporate_brand_name":"H&M","article_no":"0975939005","product_name":"Merry Trash HW mom dnm trs","quantity":1,"currency":"EUR","order_item_price":17,"vat_amount":0,"freight_amount":0,"order_status":"Delivered/Returned","order_date":"2022-06-29 17:46:00"},{"order_code":"42145308946","department_corporate_brand_name":"H&M","article_no":"1045398008","product_name":"Aiko sweater","quantity":1,"currency":"EUR","order_item_price":4,"vat_amount":0,"freight_amount":0,"order_status":"Dispatched","order_date":"2022-06-20 04:07:00"}]');



-----d3------------

dataset  sales_store;;

create table `idea-player-d-ba2a.sales_store.sales_store` (
transaction_type string,
customer_request_identifier bigint,
customer_request_date timestamp,
business_partner_no bigint not null,
haal_export_timestamp timestamp,
request_details json);

insert into `idea-player-d-ba2a.sales_store.sales_store`
(transaction_type, customer_request_identifier, customer_request_date, business_partner_no, haal_export_timestamp, request_details)
values
('APSalesStore',21650,'2019-12-15T05:00:03.000+0000',234324234,'2022-10-28T11:39:51.745+0000',json'[{"CLUB_CARD_NUMBER": "100310241399470", "RECEIPT_NO": "4130", "LOCATION_NAME": "Ny Jernbanegade", "PROD_NAME": "Caesar blouse", "CRNCY_CODE": "DKK", "CUSTOMER_RETURN_FLAG": "No", "SALES_DATE": "2021-10-01 17:18:00", "TOTAL_SALES_NO_OF_PIECES": 1, "TOTAL_SALES_NET_FCC": 179, "TOTAL_SALES_VAT_FCC": 35.8}, {"CLUB_CARD_NUMBER": "100310241399470", "RECEIPT_NO": "1759", "LOCATION_NAME": "Ny Jernbanegade", "PROD_NAME": "Smiley CELINE loose denim", "CRNCY_CODE": "DKK", "CUSTOMER_RETURN_FLAG": "No", "SALES_DATE": "2021-12-02 13:59:00", "TOTAL_SALES_NO_OF_PIECES": 1, "TOTAL_SALES_NET_FCC": 186.75, "TOTAL_SALES_VAT_FCC": 37.35}, {"CLUB_CARD_NUMBER": "100310241399470", "RECEIPT_NO": "6332", "LOCATION_NAME": "Ny Jernbanegade", "PROD_NAME": "Lic. Kimye", "CRNCY_CODE": "DKK", "CUSTOMER_RETURN_FLAG": "No", "SALES_DATE": "2021-10-30 12:27:00", "TOTAL_SALES_NO_OF_PIECES": 1, "TOTAL_SALES_NET_FCC": 135.6, "TOTAL_SALES_VAT_FCC": 27.12}, {"CLUB_CARD_NUMBER": "100310241399470", "RECEIPT_NO": "1759", "LOCATION_NAME": "Ny Jernbanegade", "PROD_NAME": "Dad tee colab smiley", "CRNCY_CODE": "DKK", "CUSTOMER_RETURN_FLAG": "No", "SALES_DATE": "2021-12-02 13:59:00", "TOTAL_SALES_NO_OF_PIECES": 1, "TOTAL_SALES_NET_FCC": 129, "TOTAL_SALES_VAT_FCC": 25.8}]'),
('APSalesStore',21651,'2019-12-15T05:00:03.000+0000',234324235,'2022-10-28T11:39:51.745+0000',json'[{"CLUB_CARD_NUMBER": "100310241399471", "RECEIPT_NO": "4131", "LOCATION_NAME": "Ny Jernbanegade", "PROD_NAME": "Caesar blouse", "CRNCY_CODE": "DKK", "CUSTOMER_RETURN_FLAG": "No", "SALES_DATE": "2021-10-01 17:18:00", "TOTAL_SALES_NO_OF_PIECES": 1, "TOTAL_SALES_NET_FCC": 179, "TOTAL_SALES_VAT_FCC": 35.8}, {"CLUB_CARD_NUMBER": "100310241399471", "RECEIPT_NO": "1759", "LOCATION_NAME": "Ny Jernbanegade", "PROD_NAME": "Smiley CELINE loose denim", "CRNCY_CODE": "DKK", "CUSTOMER_RETURN_FLAG": "No", "SALES_DATE": "2021-12-02 13:59:00", "TOTAL_SALES_NO_OF_PIECES": 1, "TOTAL_SALES_NET_FCC": 186.75, "TOTAL_SALES_VAT_FCC": 37.35}, {"CLUB_CARD_NUMBER": "100310241399470", "RECEIPT_NO": "6332", "LOCATION_NAME": "Ny Jernbanegade", "PROD_NAME": "Lic. Kimye", "CRNCY_CODE": "DKK", "CUSTOMER_RETURN_FLAG": "No", "SALES_DATE": "2021-10-30 12:27:00", "TOTAL_SALES_NO_OF_PIECES": 1, "TOTAL_SALES_NET_FCC": 135.6, "TOTAL_SALES_VAT_FCC": 27.12}, {"CLUB_CARD_NUMBER": "100310241399470", "RECEIPT_NO": "1759", "LOCATION_NAME": "Ny Jernbanegade", "PROD_NAME": "Dad tee colab smiley", "CRNCY_CODE": "DKK", "CUSTOMER_RETURN_FLAG": "No", "SALES_DATE": "2021-12-02 13:59:00", "TOTAL_SALES_NO_OF_PIECES": 1, "TOTAL_SALES_NET_FCC": 129, "TOTAL_SALES_VAT_FCC": 25.8}]');


-----d4------------
dataset club_events;

create table `idea-player-d-ba2a.club_events.ap_club_events` (
transaction_type string,
customer_request_identifier bigint,
customer_request_date timestamp,
business_partner_no bigbigint not null,
haal_export_timestamp timestamp,
request_details json);


insert into `idea-player-d-ba2a.club_events.ap_club_events`
(transaction_type, customer_request_identifier, customer_request_date, business_partner_no, haal_export_timestamp, request_details)
values
('APClubEvents',118543,'2022-11-10T05:00:05.000+0000',2324234324,'2022-11-11T14:49:55.618+0000',json'[{"customer_loyality_no": "102010202236887", "offer_name": "ES09E02_Conscious_Exclusive_Pre_Selling_Event_Mad_Bcn_18.03_31.03_0p", "offer_last_modified_timestamp": "2019-03-30 20:59:37", "offer_status": "Viewed"}, {"customer_loyality_no": "102010202236887", "offer_name": "ES09E02_Conscious_Exclusive_Pre_Selling_Event_Mad_Bcn_18.03_31.03_0p", "offer_last_modified_timestamp": "2019-03-29 11:32:31", "offer_status": "Viewed"}]'),
('APClubEvents',118544,'2022-11-10T05:00:05.000+0000',2324234325,'2022-11-11T14:49:55.618+0000',json'[{"customer_loyality_no": "102010202236888", "offer_name": "ES09E02_Conscious_Exclusive_Pre_Selling_Event_Mad_Bcn_18.03_31.03_0p", "offer_last_modified_timestamp": "2019-03-30 20:59:37", "offer_status": "Viewed"}, {"customer_loyality_no": "102010202236888", "offer_name": "ES09E02_Conscious_Exclusive_Pre_Selling_Event_Mad_Bcn_18.03_31.03_0p", "offer_last_modified_timestamp": "2019-03-29 11:32:31", "offer_status": "Viewed"}]');



----------------d5-------------
dataset sales_delivery;

create table `idea-player-d-ba2a.sales_delivery.sales_delivery_address` (
transaction_type string,
customer_request_identifier bigint,
customer_request_date timestamp,
business_partner_no bigint not null,
haal_export_timestamp timestamp,
request_details json);

insert into `idea-player-d-ba2a.sales_delivery.sales_delivery_address`
(transaction_type, customer_request_identifier, customer_request_date, business_partner_no, haal_export_timestamp, request_details)
values
('APSalesDeliveryAddress',90805,'2021-11-27T11:00:06.000+0000',22222,'2022-11-17T12:41:24.877+0000','[{"STREET_ADDRESS": "Trappenkehre 9", "STREET_NUMBER": null, "BUILDING_NAME": null, "ZIP_CODE": "2222", "CITY": "Hannover", "PROVINCE": "Niedersachsen"}, {"STREET_ADDRESS": "Trappenkehre 9", "STREET_NUMBER": null, "BUILDING_NAME": null, "ZIP_CODE": "11111", "CITY": "Hannover", "PROVINCE": null}, {"STREET_ADDRESS": "Ricklinger Stadtweg 101", "STREET_NUMBER": null, "BUILDING_NAME": null, "ZIP_CODE": "33434", "CITY": "Hannover", "PROVINCE": null}]'),
('APSalesDeliveryAddress',90806,'2021-11-27T11:00:06.000+0000',22223,'2022-11-17T12:41:24.877+0000','[{"STREET_ADDRESS": "Trappenkehre 9", "STREET_NUMBER": null, "BUILDING_NAME": null, "ZIP_CODE": "2222", "CITY": "Hannover", "PROVINCE": "Niedersachsen"}, {"STREET_ADDRESS": "Trappenkehre 9", "STREET_NUMBER": null, "BUILDING_NAME": null, "ZIP_CODE": "11111", "CITY": "Hannover", "PROVINCE": null}, {"STREET_ADDRESS": "Ricklinger Stadtweg 101", "STREET_NUMBER": null, "BUILDING_NAME": null, "ZIP_CODE": "33435", "CITY": "Hannover", "PROVINCE": null}]');



*************************************************