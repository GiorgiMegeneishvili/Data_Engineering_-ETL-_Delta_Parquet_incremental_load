-- 4️⃣ trigger on ModifiedDate
CREATE TRIGGER trg_UpdateOrInsertModifiedDate
ON dbo.person
AFTER INSERT, UPDATE
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE p
    SET ModifiedDate = GETDATE()
    FROM dbo.person p
    INNER JOIN inserted i
        ON p.BusinessEntityID = i.BusinessEntityID;
END;

-- 5️⃣ 5 UPDATE მაგალითი
UPDATE dbo.person SET LastName = 'Sánchez' WHERE BusinessEntityID = 1;
UPDATE dbo.person SET FirstName = 'Liam' WHERE BusinessEntityID = 2;
UPDATE dbo.person SET EmailPromotion = 2 WHERE BusinessEntityID = 3;
UPDATE dbo.person SET Title = 'Dr.' WHERE BusinessEntityID = 4;
UPDATE dbo.person SET MiddleName = 'giogio135413' WHERE BusinessEntityID = 5;

-- 6️⃣ 15 ახალი INSERT მაგალითი
INSERT INTO dbo.person (BusinessEntityID, PersonType, NameStyle, Title, FirstName, MiddleName, LastName, Suffix, EmailPromotion)
VALUES
(20778, 'EM', 0, 'Mr.', 'John', 'A.', 'Doe', NULL, 1),
(20779, 'EM', 1, 'Ms.', 'Jane', 'B.', 'Smith', NULL, 0),
(20780, 'EM', 0, 'Mrs.', 'Mary', 'C.', 'Johnson', NULL, 1),
(20781, 'EM', 0, 'Dr.', 'Robert', 'D.', 'Williams', NULL, 0),
(20782, 'EM', 1, 'Mr.', 'Michael', 'E.', 'Brown', NULL, 1),
(20783, 'EM', 0, NULL, 'William', 'F.', 'Jones', NULL, 0),
(20784, 'EM', 1, 'Ms.', 'Linda', 'G.', 'Garcia', NULL, 1),
(20785, 'EM', 0, NULL, 'Elizabeth', 'H.', 'Martinez', NULL, 0),
(20786, 'EM', 1, 'Mr.', 'David', 'I.', 'Rodriguez', NULL, 1),
(20787, 'EM', 0, 'Mrs.', 'Susan', 'J.', 'Lee', NULL, 0),
(20788, 'EM', 1, 'Dr.', 'James', 'K.', 'Walker', NULL, 1),
(20789, 'EM', 0, 'Ms.', 'Patricia', 'L.', 'Hall', NULL, 0),
(20790, 'EM', 0, 'Mr.', 'Charles', 'M.', 'Allen', NULL, 1),
(20791, 'EM', 1, 'Mrs.', 'Barbara', 'N.', 'Young', NULL, 0),
(20792, 'EM', 0, 'Dr.', 'Thomas', 'O.', 'Hernandez', NULL, 1);




-- last 7 Days of Data
SELECT * FROM dbo.person WHERE CAST(ModifiedDate AS DATE) >= CAST(DATEADD(DAY, -7, SYSDATETIME()) AS DATE);




SELECT * FROM dbo.person;
