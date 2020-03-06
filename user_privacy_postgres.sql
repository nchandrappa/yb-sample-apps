\c hasuratest
CREATE TABLE user_privacy (
	
	userID BIGINT NOT NULL ,
	userPrivacyID BIGINT NOT NULL ,
	userGenderPrivacy BOOLEAN ,
	userAgePrivacy BOOLEAN ,
	userDobPrivacy BOOLEAN ,
	userAddressPrivacy BOOLEAN ,
	userEmailPrivacy BOOLEAN ,
	userHomePhonePrivacy BOOLEAN ,
	mobilePhonePrivacy VARCHAR ,
	companyEmailPrivacy VARCHAR ,
	createdDate TIMESTAMP ,
	PRIMARY KEY (userID, userPrivacyID)
);

ALTER TABLE user_privacy ADD FOREIGN KEY (userID) REFERENCES user_account(userID);
