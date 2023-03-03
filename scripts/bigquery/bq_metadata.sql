*************************************************
------------metadata------------
**************************************************
create database ap_metadata;

create table `idea-player-d-ba2a.ap_metadata.view_registry` (
database_name string,
view_name string
);

insert into `idea-player-d-ba2a.ap_metadata.view_registry` (database_name, view_name)
values
('rr_response','v_rating_and_reviews_blank_response'),
('sales_response','v_online_sales_response'),
('sales_store','v_sales_store'),
('club_events','v_ap_club_events'),
('sales_delivery','v_sales_delivery_address');

-------------------------------------
create table `idea-player-d-ba2a.ap_metadata.access_request` (
UniqueTransactionID string,
TransactionDateTime timestamp,
CustomerRequestIdentifier bigint,
CustomerRequestDateTime timestamp,
BusinessPartnerIdentifier bigint,
status string,
attempt_count int,
created_at timestamp,
updated_at timestamp,
input_json string
);

insert into `idea-player-d-ba2a.ap_metadata.access_request` 
(UniqueTransactionID, TransactionDateTime, CustomerRequestIdentifier, CustomerRequestDateTime, BusinessPartnerIdentifier, status, attempt_count, created_at, updated_at, input_json)
values
('5d5a5d57-a8c9-4558-80ff-e3b55b45d7e9','2022-11-22T10:00:27.428Z', 119973, '2022-11-22T10:00:27.428Z', 3079352201, 'completed', 0, CURRENT_TIMESTAMP,  CURRENT_TIMESTAMP, 'test'),
('5d5a5d57-a8c9-4558-80ff-e3b55b45d7e9','2022-11-22T10:00:27.428Z', 119974, '2022-11-22T10:00:27.428Z', 3079352202, 'failure', 0, CURRENT_TIMESTAMP,  CURRENT_TIMESTAMP, 'test');

-------------------------------------
create table `idea-player-d-ba2a.ap_metadata.access_request_attempt` (
BusinessPartnerIdentifier bigint,
status string,
created_at timestamp,
updated_at timestamp
);

insert into `idea-player-d-ba2a.ap_metadata.access_request_attempt` 
(BusinessPartnerIdentifier, status, created_at)
values
(3079352202, 'failure', CURRENT_TIMESTAMP);


---------------------------------------
ALTER TABLE  `idea-player-d-ba2a.ap_metadata.access_request` ADD PRIMARY KEY (BusinessPartnerIdentifier) NOT ENFORCED;

ALTER TABLE `idea-player-d-ba2a.ap_metadata.access_request_attempt`
ADD CONSTRAINT foreign_key FOREIGN KEY (BusinessPartnerIdentifier)
REFERENCES `idea-player-d-ba2a.ap_metadata.access_request`(BusinessPartnerIdentifier) NOT ENFORCED

-------------------------------------------
************************************************