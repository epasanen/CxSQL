USE [CxDB]
GO

/****** Object:  View [CxEntities].[Scan]    Script Date: 10/9/2020 7:54:09 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE VIEW [CxEntities].[v_GetPathAndNodes]
(

DECLARE @ScanID AS BIGINT = 1020024
DECLARE @QueriesFilter AS NVARCHAR(MAX)
DECLARE @SeveritiesFilter AS NVARCHAR(MAX)
DECLARE @StatesFilter AS NVARCHAR(MAX)
DECLARE @AssignedToFilter AS NVARCHAR(MAX)
DECLARE @CategoriesFilter AS NVARCHAR(MAX)

BEGIN

DECLARE @CurrentScanResultID AS BIGINT;
DECLARE @ProjectID AS BIGINT;
DECLARE	@ScanVisibility AS BIT;
DECLARE	@Owner AS NVARCHAR(120);
DECLARE @PreviousScanResultID AS BIGINT;
DECLARE @StatePerTeams AS BIT;


DECLARE @NewStatus AS INT;
SET @NewStatus = 2;
DECLARE @Reccurent AS INT;
SET @Reccurent = 1;
--DECLARE @FirstandLast table(id int identity(1,1), QueryVersionCode int, QuerySeverity int, Path_Id int, Similarity_Hash bigint, ResultStatus int, ResultSeverity int, ResultState int, ResultAssignedTo varchar(255),Node_Id int, Full_Name varchar(255), Short_Name varchar(100), [File_Name] varchar(100), Line int, Col int, Length int, DOM_id int, Method_Line int)

IF OBJECT_ID(N'tempdb..#ScanResultsLabels') IS NOT NULL
BEGIN
DROP TABLE #ScanResultsLabels
END
IF OBJECT_ID(N'tempdb..#GetScanReportResults') IS NOT NULL
BEGIN
DROP TABLE #GetScanReportResults
END

--Get project id and result ID
SELECT @ProjectID = TaskScans.ProjectId, @CurrentScanResultID = TaskScans.ResultId, @ScanVisibility =IsPublic, @Owner=Owner  FROM TaskScans WHERE TaskScans.Id = @ScanID;

--Get previous scan ------------------------------------------------------------------------------------
SELECT  @PreviousScanResultID = ISNULL(MAX(PreviousScan.ResultId) , -1) 
FROM TaskScans AS PreviousScan
WHERE PreviousScan.ProjectId = @ProjectID
   AND PreviousScan.Id < @ScanID 
   AND PreviousScan.ScanType = 1  /*Regular scan*/
   AND PreviousScan.is_deprecated = 0
   AND (PreviousScan.IsPublic =1 OR (@ScanVisibility=0 AND @Owner = PreviousScan.Owner));
--------------------------------------------------------------------------------------------------------

---Set states per project or team
SELECT @StatePerTeams = (CASE CxComponentConfiguration.Value  WHEN 'true' THEN 1 ELSE 0 END)
FROM CxComponentConfiguration 
WHERE CxComponentConfiguration.[Key]  = 'RESULT_ATTRIBUTES_PER_SIMILARITY';


---Retrieve latest result labels
WITH scanLabels AS
(
SELECT  ResultsLabels.SimilarityID 
       ,ResultsLabels.LabelType 
       ,ResultsLabels.NumericData
       ,ResultsLabels.StringData
	   ,row_number() over (partition by ResultsLabels.SimilarityID ,ResultsLabels.LabelType  order by ResultsLabels.[UpdateDate] desc) AS RowNumber
FROM ResultsLabels
WHERE  ResultsLabels.LabelType IN (2,3,4)
  AND  ((@StatePerTeams = 1 AND ResultsLabels.ProjectId IN (SELECT TeamProjects.Id FROM Projects AS TeamProjects WHERE TeamProjects.Owning_Team = ( SELECT Projects.Owning_Team FROM Projects WHERE  Projects.Id = @ProjectID)))
        OR 
       (@StatePerTeams = 0 AND  ResultsLabels.ProjectId = @ProjectID))
)
SELECT SimilarityID ,LabelType , StringData, NumericData INTO #ScanResultsLabels
FROM  scanLabels 
WHERE RowNumber = 1;

--------------------------------------

SELECT * INTO #GetScanReportResults FROM (
	SELECT    CurrentScanPathResults.QueryVersionCode
			  ,ResultQuery.Severity AS  QuerySeverity
	          ,CurrentScanPathResults.Path_Id 
	          ,CurrentScanPathResults.Similarity_Hash
	          , CASE 
					WHEN PreviousScanPathResults.Similarity_Hash IS NULL THEN @NewStatus 
					ELSE @Reccurent
	            END 
	          AS ResultStatus
	          , CAST(ISNULL(ResultLabelsSeverity.NumericData,ResultQuery.Severity) AS INT) AS ResultSeverity 
	          , CAST(ISNULL(ResultLabelsState.NumericData,0) AS INT) AS ResultState
	          , ISNULL(ResultLabelsAssignedTo.StringData, N'') AS ResultAssignedTo
	FROM     PathResults AS CurrentScanPathResults 
		INNER JOIN QueryVersion AS ResultQuery ON ResultQuery.QueryVersionCode = CurrentScanPathResults.QueryVersionCode
		LEFT OUTER JOIN (SELECT p.Similarity_Hash , p.QueryVersionCode 
		                 FROM PathResults AS P 
		                 WHERE p.ResultId = @PreviousScanResultID GROUP BY p.Similarity_Hash , p.QueryVersionCode
		                 ) AS PreviousScanPathResults 
		                   ON PreviousScanPathResults.Similarity_Hash = CurrentScanPathResults.Similarity_Hash 
		                  AND PreviousScanPathResults.Similarity_Hash <> 0                    
		                  AND PreviousScanPathResults.QueryVersionCode = CurrentScanPathResults.QueryVersionCode
		LEFT OUTER JOIN #ScanResultsLabels AS ResultLabelsSeverity ON ResultLabelsSeverity.SimilarityID  =   CurrentScanPathResults.Similarity_Hash   AND ResultLabelsSeverity.LabelType = 2               
		LEFT OUTER JOIN #ScanResultsLabels AS ResultLabelsState ON ResultLabelsState.SimilarityID  =   CurrentScanPathResults.Similarity_Hash   AND ResultLabelsState.LabelType = 3               
		LEFT OUTER JOIN #ScanResultsLabels AS ResultLabelsAssignedTo ON ResultLabelsAssignedTo.SimilarityID  =   CurrentScanPathResults.Similarity_Hash   AND ResultLabelsAssignedTo.LabelType = 4             
	WHERE CurrentScanPathResults.ResultId = @CurrentScanResultID 
	  AND CurrentScanPathResults.Similarity_Hash <> 0 
	  AND (@QueriesFilter IS NULL OR ResultQuery.QueryId IN(SELECT * FROM Split(@QueriesFilter,',')))
	  AND (@CategoriesFilter IS NULL OR CurrentScanPathResults.QueryVersionCode IN(SELECT DISTINCT QueryVersion.QueryVersionCode 
																		   FROM QueryVersion 
																				LEFT JOIN CategoryForQuery ON QueryVersion.QueryId = CategoryForQuery.QueryId
																				INNER JOIN (SELECT CAST(splitdata AS INT) AS CategoryID 
																		                    FROM Split(@CategoriesFilter,',')) AS Categories 
																					ON Categories.CategoryID = ISNULL(CategoryForQuery.CategoryId,0)))
	  ) AS Data
WHERE (@SeveritiesFilter IS NULL OR ResultSeverity IN(SELECT * FROM Split(@SeveritiesFilter,',')))  
  AND (@StatesFilter IS NULL OR  ResultState IN (SELECT * FROM Split(@StatesFilter,',')))
  AND (@AssignedToFilter IS NULL OR ResultAssignedTo IN(SELECT * FROM Split(@AssignedToFilter,',')));

SELECT 
		  [Q].[Name] as QueryName
		  ,[CR].[Path_Id]
          ,[NR].[Node_id] 
		  ,[CR].ResultSeverity
		  ,[CR].ResultState

		  ,[CR].ResultStatus
	      ,[NR].[Full_Name] 
		  ,[NR].[Short_Name]
		  ,[NR].[File_Name]
          ,[NR].[Line]
          ,[NR].[Col]
          ,[NR].[Length]
          ,[NR].[DOM_Id]
          ,[NR].[Method_Line]
FROM     #GetScanReportResults AS CR
	INNER JOIN NodeResults NR ON NR.ResultId = @CurrentScanResultID AND NR.Path_Id = CR.Path_Id
	JOIN [dbo].[QueryVersion] Q ON Q.QueryVersionCode = CR.QueryVersionCode
WHERE 
NR.[Node_Id] = (SELECT MIN(Node_Id) FROM NodeResults WHERE Path_Id = CR.Path_Id) 
OR
NR.[Node_Id] = (SELECT MAX(Node_Id) FROM NodeResults WHERE Path_Id = CR.Path_Id)

ORDER BY [NR].Path_Id, [NR].Node_Id;

END
)


