
USE CxDB

SET  NOCOUNT ON

Declare @StartDate datetime = '01-01-2017'
Declare @EndDate datetime = '12-31-2021'
Declare @projectId int
Declare @TopNode bit = 1
Declare @projectName nvarchar(255)
Declare @scanCount int
Declare @presetId int
DECLARE	@ScanVisibility AS BIT;
DECLARE	@Owner AS NVARCHAR(120);
DECLARE @CurrentScanResultID AS BIGINT;
DECLARE @PreviousScanResultID AS BIGINT;
DECLARE @StatePerTeams AS BIT;
DECLARE @ScanCompletedDate datetime


DECLARE @NewStatus AS nvarchar(10);
SET @NewStatus = 'New';
DECLARE @Reccurent AS nvarchar(10);
SET @Reccurent = 'Recurring'

Declare @Severity table(id int, value varchar(20))
Insert into @Severity
	Select 0,'Informational'
Insert into @Severity
	Select 1,'Low'
Insert into @Severity
	Select 2,'Medium'
Insert into @Severity
	Select 3,'High'



Declare @ScanResultsLabels table(SimilarityID bigint ,LabelType nvarchar(255), NumericData int, StringData nvarchar(255), RowNumber int)
Declare @GetScanReportResults table(QueryVersionID bigint, QuerySeverity int, Path_Id int, Similarity_Hash bigint, ResultStatus nvarchar(20), ResultSeverity int, ResultState nvarchar(20), ResultAssignedTo nvarchar(255))


Declare @teams table (teamId nvarchar(255) ,teamName nvarchar(255), teampath nvarchar(255), fullname nvarchar(255))
Insert Into @teams
 Select T.Id, T.[Name], T.Path, T.FullName from [CxDB].[CxEntities].[Team] T

Declare @presets table (presetId bigint ,presetName nvarchar(255))
Insert Into @presets
 Select P.Id, P.Name from [CxDB].[dbo].Presets P

Declare @projects table(projectId bigint, projectName nvarchar(255), OwningTeamName int, PresetID int, OpenedAt datetime)
Insert into @projects
 Select PR.Id, PR.Name, PR.Owning_Team, PR.PresetId, PR.OpenedAt from [CxDB].[dbo].[Projects] PR where OpenedAt between @StartDate and @EndDate

Declare @TaskScans table(ScanID bigint, ProjectId bigint, ResultId bigint, IsPublic bit, [Owner] nvarchar(100), ScanCompletedOn datetime, RowNumber int)
Declare @ScanRemarks table (ProjectID bigint, ResultId bigint, PathID int, SimilarityId bigint ,CreatedOn datetime ,ProjectName nvarchar(100), Userdata nvarchar(255), Comment nvarchar(max), Merged nvarchar(max), RowNumber int)

Declare @allscans table(
 projectName nvarchar(255), teamName nvarchar(255), presetName nvarchar(255), similarityId bigint, 
 resultId bigint, pathId bigint, nodeId bigint, isFalsePositive nvarchar(10), projectId bigint,
 queryId bigint, scanId bigint, [state] nvarchar(20), [status] nvarchar(100), [severity] nvarchar(10), [group] nvarchar(100), 
 query nvarchar(100), [lineNo] int, [column] int, [firstLine] nvarchar(1024), [fileName] nvarchar(1024), deepLink nvarchar(1024), 
 nodeName nvarchar(100), remark varchar(1024),scanDate datetime
 )

---Set states per project or team
SELECT @StatePerTeams = (CASE CxComponentConfiguration.Value  WHEN 'true' THEN 1 ELSE 0 END)
FROM CxComponentConfiguration 
WHERE CxComponentConfiguration.[Key]  = 'RESULT_ATTRIBUTES_PER_SIMILARITY';




IF CURSOR_STATUS('global','cxsast_results')>=-1
DEALLOCATE cxsast_results


Declare cxsast_results CURSOR FOR
  Select projectId, projectName, PresetID
  FROM @projects

Open cxsast_results
Fetch NEXT FROM cxsast_results INTO 
    @projectId, 
    @projectName,
	@presetId

While @@FETCH_STATUS = 0
    BEGIN
		Print Cast(@projectID as varchar) + ' - ' + @projectName + ' - ' + Cast(@StartDate as varchar) + ' ' + Cast(@EndDate as varchar)	
				
			
		Declare @Company nvarchar(255) 
		Declare @Team nvarchar(255)
		Declare @ScanId bigint
		Declare @presetName nvarchar(100)

		Declare @name NVARCHAR(255)
        Declare @pos INT
		Declare @scount int = 1
		Declare @id int = 1
		Declare @Split table(id int, name nvarchar(100))


		Declare @TeamString nvarchar(255)
		Select @TeamString = T.fullname, @Team=T.[teamName] from @teams T where teamId in (Select P.OwningTeamName from @projects P where P.projectId = @ProjectID)
		Select @presetName = presetName From @presets where presetId = @presetId

		SET @id = 1


		Delete from @TaskScans

		Insert into @TaskScans
			SELECT 
			Id,
			ProjectId, 
			ResultId, 
			IsPublic,
			[Owner],
			ScanRequestCompletedOn,
			row_number() over (order by TaskScans.ResultId)
			FROM TaskScans WHERE TaskScans.ProjectId = @ProjectId
		
		Select @scount = count(ScanId) from @TaskScans
		Print 'Scan Count : ' +  Cast(@scount as nvarchar(20))

		Delete from @ScanRemarks

		Insert into @ScanRemarks
		SELECT  DISTINCT  
		@projectId,
		@CurrentScanResultID,
		ResultsLabels.PathID,
		ResultsLabels.SimilarityId ,
		ResultsLabelsHistory.DateCreated,
		ISNULL(Projects.Name,' '), 
		ISNULL(users.FirstName + ' ' + users.LastName, ResultsLabelsHistory.CreatedBy),
		ISNULL(ResultsLabelsHistory.Data , ''),
		ISNULL(users.FirstName + ' ' + users.LastName, ResultsLabelsHistory.CreatedBy) + ' ' + @projectname + ',[' +  FORMAT(getdate(), 'dddd, MMMM, yyyy hh:mm') + ']: ' +  ISNULL(ResultsLabelsHistory.Data , ''),
		row_number() over (order by ResultsLabels.ResultId asc)
		FROM ResultsLabels
		INNER JOIN ResultsLabelsHistory  ON ResultsLabelsHistory.ID = ResultsLabels.ID 
		LEFT JOIN Projects ON Projects.Id = ResultsLabels.ProjectId
		LEFT JOIN Users ON Users.UserName = ResultsLabelsHistory.CreatedBy
		WHERE  
		     ResultsLabels.NumericData is null AND
			(@StatePerTeams = 1 AND ResultsLabels.ProjectId IN (SELECT TeamProjects.Id FROM Projects AS TeamProjects WHERE TeamProjects.Owning_Team = ( SELECT Projects.Owning_Team FROM Projects WHERE  Projects.Id = @ProjectID)))
			OR 
			(@StatePerTeams = 0 AND  ResultsLabels.ProjectId = @ProjectID) AND ResultsLabels.LabelType = 1
		ORDER BY ResultsLabelsHistory.DateCreated DESC


		While (@id <= @scount)
		BEGIN
			Delete from @GetScanReportResults

			SELECT @ScanID = ScanId, @CurrentScanResultID = ResultId, @ScanVisibility =IsPublic, @Owner=Owner, @ScanCompletedDate = ScanCompletedOn  FROM @TaskScans WHERE ProjectID = @ProjectId and RowNumber = @id
			AND ScanCompletedOn between @StartDate and @EndDate

			Print 'ScanId : ' +  Cast(@ScanId as nvarchar(20))
					
		   --Get previous scan ------------------------------------------------------------------------------------		
			SELECT  @PreviousScanResultID = ISNULL(MAX(PreviousScan.ResultId) , -1) 
			FROM TaskScans AS PreviousScan
			WHERE PreviousScan.ProjectId = @ProjectID
				AND PreviousScan.Id < @ScanID 
				AND PreviousScan.ScanType = 1  /*Regular scan*/
				AND PreviousScan.is_deprecated = 0
				AND (PreviousScan.IsPublic =1 OR (@ScanVisibility=0 AND @Owner = PreviousScan.Owner));
			--------------------------------------------------------------------------------------------------------
			---Retrieve latest result labels
			INSERT INTO @ScanResultsLabels

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

			--------------------------------------

			INSERT INTO @GetScanReportResults
				SELECT   
							CurrentScanPathResults.QueryVersionCode
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
					LEFT OUTER JOIN @ScanResultsLabels AS ResultLabelsSeverity ON ResultLabelsSeverity.SimilarityID  =   CurrentScanPathResults.Similarity_Hash   AND ResultLabelsSeverity.LabelType = 2 AND ResultLabelsSeverity.RowNumber = 1
					LEFT OUTER JOIN @ScanResultsLabels AS ResultLabelsState ON ResultLabelsState.SimilarityID  =   CurrentScanPathResults.Similarity_Hash   AND ResultLabelsState.LabelType = 3 AND ResultLabelsState.RowNumber  = 1            
					LEFT OUTER JOIN @ScanResultsLabels AS ResultLabelsAssignedTo ON ResultLabelsAssignedTo.SimilarityID  =   CurrentScanPathResults.Similarity_Hash   AND ResultLabelsAssignedTo.LabelType = 4 AND ResultLabelsAssignedTo.RowNumber = 1           
				WHERE CurrentScanPathResults.ResultId = @CurrentScanResultID 
					AND CurrentScanPathResults.Similarity_Hash <> 0 


			Insert Into @allScans
			SELECT DISTINCT
						@projectName
						,@Team
						,@presetName
						,CurrentScanPathResults.Similarity_Hash
						,@CurrentScanResultID
						,CurrentScanPathResults.Path_Id
						,[NodeResults].Node_Id
						,(Case When CurrentScanPathResults.ResultState = 1 then 'TRUE' else 'FALSE' end)
						,@projectId
						,QueryVersion.QueryId
						,@ScanId
						,ResultState.[Name]
						,CurrentScanPathResults.ResultStatus
						,(select [value] from @Severity SV where SV.Id = CurrentScanPathResults.ResultSeverity)
						,QueryGroup.[Name]
						,Query.[Name]
						,[NodeResults].[Line]
						,[NodeResults].[Col]
						,''
						,[NodeResults].[File_Name]
						,''
						,[NodeResults].[Short_Name]
						,(SELECT SR.Merged + ';' AS [text()]
								 FROM @ScanRemarks SR WHERE SR.SimilarityId=CurrentScanPathResults.Similarity_Hash ORDER BY SR.RowNumber FOR XML PATH (''))
						,@ScanCompletedDate
			FROM     @GetScanReportResults AS CurrentScanPathResults 
				JOIN NodeResults ON NodeResults.ResultId = @CurrentScanResultID AND NodeResults.Path_Id = CurrentScanPathResults.Path_Id
				JOIN QueryVersion ON QueryVersion.QueryVersionCode = CurrentScanPathResults.QueryVersionId
				JOIN Query ON Query.QueryId = QueryVersion.QueryId
				JOIN QueryGroup ON QueryGroup.PackageId = Query.PackageId
				JOIN ResultState ON ResultState.Id = CurrentScanPathResults.ResultState and ResultState.LanguageId = 1033
				WHERE [Node_Id] = (Case when @TopNode = 1 then 1 else [Node_Id] end)

		SET @id = @id + 1
	END

		FETCH NEXT FROM cxsast_results INTO 
			@projectId, 
			@projectName,
			@presetId
	END

Select * from @allScans order by projectid, resultId,pathId,nodeId

CLOSE cxsast_results
DEALLOCATE cxsast_results
