CREATE TABLE dbo.ETL_Runs (
Run_ID INT IDENTITY(1,1) PRIMARY KEY,
ETL_Job_Name VARCHAR(100) NOT NULL,   
Start_DateTime DATETIME2(0) NOT NULL,
Duration_Seconds  INT NULL,       
Status VARCHAR(20) NOT NULL,  
Records_Processed INT NULL,
Records_Rejected  INT NULL,
Notes NVARCHAR(MAX) NULL        
);
 
-- 
CREATE INDEX IX_ETL_Runs_JobDate ON dbo.ETL_Runs (ETL_Job_Name, Start_DateTime);
 
 
-- CATÁLOGO DE REGLAS DE VALIDACIÓN
CREATE TABLE dbo.Validation_Rules (
Rule_ID INT IDENTITY(1,1) PRIMARY KEY,
Rule_Name VARCHAR(100) NOT NULL,   
Created_Date DATETIME2(0) NOT NULL CONSTRAINT DF_Validation_Rules_CreatedDate DEFAULT (SYSDATETIME()),
Rule_Description NVARCHAR(500) NOT NULL,   
Rule_Category VARCHAR(50) NOT NULL,  
Severity_Level VARCHAR(20) NOT NULL,   
Rule_Logic NVARCHAR(MAX) NOT NULL,  
Is_Active BIT NOT NULL CONSTRAINT DF_Validation_Rules_IsActive DEFAULT (1)
);
 
-- 
CREATE UNIQUE INDEX UX_Validation_Rules_RuleName ON dbo.Validation_Rules(Rule_Name);
 
 
-- 
CREATE TABLE dbo.Validation_Results (
Result_ID INT IDENTITY(1,1) PRIMARY KEY,
Run_ID INT NOT NULL,  
Rule_ID INT NOT NULL,  
Validation_DateTime DATETIME2(0) NOT NULL CONSTRAINT DF_Validation_Results_ValDate DEFAULT (SYSDATETIME()),
Records_Checked INT NULL,
Records_Passed INT NULL,
Records_Failed INT NULL
);
 
ALTER TABLE dbo.Validation_Results
ADD CONSTRAINT FK_Validation_Results_ETL_Runs 
FOREIGN KEY (Run_ID) REFERENCES dbo.ETL_Runs(Run_ID);
 
ALTER TABLE dbo.Validation_Results
ADD CONSTRAINT FK_Validation_Results_Validation_Rules 
FOREIGN KEY (Rule_ID) REFERENCES dbo.Validation_Rules(Rule_ID);
 
-- 
CREATE INDEX IX_Validation_Results_RunID ON dbo.Validation_Results(Run_ID);
 
-- 
CREATE INDEX IX_Validation_Results_RuleID ON dbo.Validation_Results(Rule_ID);
 
 
-- 
CREATE TABLE dbo.Data_Lineage (
Lineage_ID INT IDENTITY(1,1) PRIMARY KEY,
Source_System VARCHAR(100) NOT NULL,   
Source_Table VARCHAR(100) NOT NULL,
Target_System VARCHAR(100) NOT NULL,   
Target_Table VARCHAR(100) NOT NULL,
Transformation_Description NVARCHAR(MAX) NULL,    
Last_Updated DATETIME2(0) NOT NULL CONSTRAINT DF_Data_Lineage_LastUpdated DEFAULT (SYSDATETIME())
);
 
 
CREATE INDEX IX_Data_Lineage_SourceTarget ON dbo.Data_Lineage(Source_System, Source_Table, Target_System, Target_Table);
