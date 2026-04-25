CREATE DATABASE demoDB;

use demoDB;

-- 1️⃣ DROP TABLE IF EXISTS 
DROP TABLE IF EXISTS dbo.person;

-- 2️⃣ CREATING TABLE
CREATE TABLE dbo.person (
    BusinessEntityID INT PRIMARY KEY,
    PersonType        CHAR(2),
    NameStyle         BIT,
    Title              VARCHAR(20),
    FirstName         VARCHAR(100),
    MiddleName        VARCHAR(100),
    LastName          VARCHAR(100),
    Suffix             VARCHAR(20),
    EmailPromotion    INT,
    ModifiedDate      DATETIME DEFAULT GETDATE()
);


-- 3️⃣ INSERTING DATA FROM  -----> AdventureWorks2022.Person.Person
INSERT INTO dbo.person (
    BusinessEntityID,
    PersonType,
    NameStyle,
    Title,
    FirstName,
    MiddleName,
    LastName,
    Suffix,
    EmailPromotion,
    ModifiedDate
)
SELECT
    BusinessEntityID,
    PersonType,
    NameStyle,
    Title,
    FirstName,
    MiddleName,
    LastName,
    Suffix,
    EmailPromotion,
    ModifiedDate
FROM AdventureWorks2022.Person.Person;
