
--1. Dataset AccountDetails

--Table
create table `idea-player-d-ba2a.AccountDetails.AccountDetails` (
CustomerID string not null,
FirstName string,
LastName string,
DateofBirth datetime,
PrefixPhone_Number string,
PhoneNumber string,
EmailAddress string,
Street string,
Street2 string,
HouseNo string,
PostalCode string,
Town_City string,
Region string,
District string,
Country string,
Nationalidentificationnumber bigint,
Gender string,
Language1 string
);

-- VIEW
create view `idea-player-d-ba2a.AccountDetails.v_AccountDetails` as select * from `idea-player-d-ba2a.AccountDetails.AccountDetails`;

--2. Dataset LegalGrounds

create table `idea-player-d-ba2a.LegalGrounds.LegalGrounds`
(
business_partner_no bigint not null,
LegalGrounds string, 
Status string,
Channel string,
Date datetime
);

--VIEW
create view `idea-player-d-ba2a.LegalGrounds.v_LegalGrounds` as select * from `idea-player-d-ba2a.LegalGrounds.LegalGrounds`;


--3. Dataset Customer_service_cases

create table `idea-player-d-ba2a.Customer_service_cases.Customer_service_cases`
(
business_partner_no bigint not null,
Description string,
Status string,
Date datetime);

--VIEW
create view `idea-player-d-ba2a.Customer_service_cases.v_Customer_service_cases` as select * from `idea-player-d-ba2a.Customer_service_cases.Customer_service_cases`;


--4. Dateset Membership_Information

create table `idea-player-d-ba2a.Membership_Information.Membership_Information`
(

business_partner_no bigint not null,
MembershipID string,
MemberSince timestamp,
MembershipStatus string,
FashionNews string,
ChildInfo string,
MembershipPoints DECIMAL
);

--View 
create view `idea-player-d-ba2a.Membership_Information.v_Membership_Information` as select * from `idea-player-d-ba2a.Membership_Information.Membership_Information`;


--5. Dataset Points_History

create table `idea-player-d-ba2a.Points_History.Points_History`
(
business_partner_no bigint not null,
Date datetime,
ActivityDescription string,
Campaign string,
Reason string,
StoreReceipt_OnlineOrderNumber string,
Store string, 
Points DECIMAL
);

create view `idea-player-d-ba2a.Points_History.v_Points_History` as select * from `idea-player-d-ba2a.Points_History.Points_History`;



--6.Dateset Tier_Information

create table `idea-player-d-ba2a.Tier_Information.Tier_Information`
(
business_partner_no bigint not null,
TierLevel string, 
TierStartDate datetime,
TierEndDate datetime,
TierExpiryDate datetime,
CurrentYearEarnedPoints DECIMAL,
CurrentYearConvertedPoints decimal,
PreviousYearEarnedPoints DECIMAL,
PreviousYearConvertedPoints Decimal
);

--View
create view `idea-player-d-ba2a.Tier_Information.v_Tier_Information` as select * from `idea-player-d-ba2a.Tier_Information.Tier_Information`;

--7. dataset Bonus_Voucher

create table  `idea-player-d-ba2a.Bonus_Voucher.Bonus_Voucher`
(
business_partner_no bigint not null,
SNo INTEGER,
Type_CreatedBy string,
Value DECIMAL,
Currency string,
Status string,
StatusReason string,
PointsUsed decimal,
ValidFrom datetime,
ValidTo datetime);

--VIEW
create view `idea-player-d-ba2a.Bonus_Voucher.v_Bonus_Voucher` as select * from `idea-player-d-ba2a.Bonus_Voucher.Bonus_Voucher`;

--8. dateset Online_Orders_and_Returns

create table `idea-player-d-ba2a.Online_Orders_and_Returns.Online_Orders_and_Returns`
(
business_partner_no bigint not null,
OrderNumber bigint,
Product string,
Quantity int,
Currency string,
Price DECIMAL,
ShippingFee DECIMAL,
VAT DECIMAL,
Status string,
Date datetime
);
--VIEW
create view  `idea-player-d-ba2a.Online_Orders_and_Returns.v_Online_Orders_and_Returns` as select * from `idea-player-d-ba2a.Online_Orders_and_Returns.Online_Orders_and_Returns`;

--9. dateset Member_Events

create table `idea-player-d-ba2a.Member_Events.Member_Events`
(
business_partner_no bigint not null,
EventName string,
EventStatus string,
Date datetime);

--view
create view `idea-player-d-ba2a.Member_Events.v_Member_Events`
AS select * from `idea-player-d-ba2a.Member_Events.Member_Events`;


--10. Dataset Ratings_and_Reviews

create table `idea-player-d-ba2a.Ratings_and_Reviews.Ratings_and_Reviews`
(
business_partner_no bigint not null,
OrderNumber bigint,
ProductName string,
Size string,
DateOfPurchase datetime,
DateOfReview datetime,
StarRating int,
ReviewStatus string
);

create view `idea-player-d-ba2a.Ratings_and_Reviews.v_Ratings_and_Reviews` as select * from `idea-player-d-ba2a.Ratings_and_Reviews.Ratings_and_Reviews`;

--11. Dataset Delivery_Addresses

create table `idea-player-d-ba2a.Delivery_Addresses.Delivery_Addresses`
(
business_partner_no bigint not null,
Receivername string,
Receiveralternativename string,
C_o string,
Pick_up_collectpointaddress string,
Deliveryaddress string
);

create view 
`idea-player-d-ba2a.Delivery_Addresses.v_Delivery_Addresses` as select * from `idea-player-d-ba2a.Delivery_Addresses.Delivery_Addresses`;



--12. Dateset Customer_Tickets

create table `idea-player-d-ba2a.Customer_Tickets.Customer_Tickets`

(
business_partner_no bigint not null,
TicketID string,
Category string,
Status string);

create view `idea-player-d-ba2a.Customer_Tickets.v_Customer_Tickets` as select * from `idea-player-d-ba2a.Customer_Tickets.Customer_Tickets`;
