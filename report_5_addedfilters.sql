
USE CxDB

SET  NOCOUNT ON
-- Set parameters prior to running
Declare @StartDate datetime = Convert(datetime, '09-01-2020',101)
Declare @EndDate datetime = Convert(datetime, '12-31-2021', 101)
Declare @CxTeam nvarchar(100) = null
Declare @CxProject nvarchar(100) = ''
Declare @CxPreset nvarchar(50) = null
Declare @CxFileExt nvarchar(50) = '.jsp'
Declare @isSummary nvarchar(10) = 'false'
-- Do not modify anything below this line
Declare @projectId int
Declare @TopNode bit = 1
Declare @projectName nvarchar(255)
Declare @scanCount int
Declare @vulerabilityCount int
Declare @presetId int
DECLARE	@ScanVisibility AS BIT;
DECLARE	@Owner AS NVARCHAR(120);
DECLARE @CurrentScanResultID AS BIGINT;
DECLARE @PreviousScanResultID AS BIGINT;
DECLARE @NextScanResultID AS BIGINT;
DECLARE @StatePerTeams AS BIT;
DECLARE @ScanCompletedDate datetime
	
Declare @Company nvarchar(255) 
Declare @Team nvarchar(255)
Declare @ScanId bigint
Declare @presetName nvarchar(100)

Declare @name NVARCHAR(255)
Declare @pos INT
Declare @id int = 1

Declare @firstFound datetime
Declare @lastFound datetime
Declare @firstFoundScan datetime
Declare @lastFoundScan datetime
Declare @firstScanId bigint
Declare @lastScanId bigint
Declare @keySID bigint
Declare @keyFile nvarchar(max)
Declare @isToday datetime = getDate()

Declare @firstFalsePositive varchar(100)
Declare @firstSingularityId bigint
Declare @firstFileName nvarchar(max)
Declare @newScanID int
Declare @fixedScanID int
Declare @NotExploitableScanID int
Declare @recurringScanID int

Declare @resultStatus nvarchar(30)
Declare @TeamString nvarchar(255)


DECLARE @NewStatus AS nvarchar(10);
SET @NewStatus = 'New';
DECLARE @Reccurent AS nvarchar(10);
SET @Reccurent = 'Recurring'
DECLARE @Fixed AS nvarchar(10);
SET @Fixed = 'Fixed'

Declare @Severity table(id int, value varchar(20))
Insert into @Severity
	Select 0,'Informational'
Insert into @Severity
	Select 1,'Low'
Insert into @Severity
	Select 2,'Medium'
Insert into @Severity
	Select 3,'High'


IF OBJECT_ID('tempdb..#ScanResultsLabels') IS NOT NULL DROP TABLE #ScanResultsLabels

CREATE TABLE #ScanResultsLabels(SimilarityID bigint ,LabelType nvarchar(255), NumericData int, StringData nvarchar(255), RowNumber int)

IF OBJECT_ID('tempdb..#GetScanReportResults') IS NOT NULL DROP TABLE #GetScanReportResults

CREATE TABLE #GetScanReportResults(QueryVersionID bigint, QuerySeverity int, Path_Id int, CurrentSimilarity_Hash bigint,  PreviousSimilarity_Hash bigint,  NextSimilarity_Hash bigint, ResultStatus nvarchar(20), ResultSeverity int, ResultState nvarchar(20), ResultAssignedTo nvarchar(255))

IF OBJECT_ID('tempdb..#teams') IS NOT NULL DROP TABLE #teams

CREATE TABLE #teams(teamId nvarchar(max) ,teamName nvarchar(255), teampath nvarchar(255), fullname nvarchar(255))
Insert Into #teams
 Select T.Id, T.[Name], T.Path, T.FullName from [CxDB].[CxEntities].[Team] T

 IF OBJECT_ID('tempdb..#presets') IS NOT NULL DROP TABLE #presets

CREATE TABLE #presets (presetId bigint ,presetName nvarchar(255))
Insert Into #presets
 Select P.Id, P.Name from [CxDB].[dbo].Presets P

 IF OBJECT_ID('tempdb..#projects') IS NOT NULL DROP TABLE #projects

CREATE TABLE #projects(projectId bigint, projectName nvarchar(255), OwningTeamName nvarchar(100), PresetID int, OpenedAt datetime)

--select * from @teams where fullname like '%' + @CxTeam + '%'
--select @Startdate
--select @EndDate

--Select PR.Id, PR.Name, PR.Owning_Team, PR.PresetId, PR.OpenedAt from [CxDB].[dbo].[Projects] PR where OpenedAt between @StartDate and @EndDate
--Select * from [CxDB].[CxEntities].[Team] T


Insert into #projects
 Select PR.Id, PR.Name, PR.Owning_Team, PR.PresetId, PR.OpenedAt from [CxDB].[dbo].[Projects] PR where PR.is_deprecated != 1

If isnull(@CxTeam,'') != ''
  Delete from #projects where OwningTeamName not in (select teamId from #teams where fullname like '%' + @CxTeam + '%')

If isnull(@CxProject,'') != ''
  Delete from #projects where ProjectName not like '%' + @CxProject + '%'


--select * from @projects order by OwningTeamName
--Select * from [CxDB].[dbo].[Projects] PR where ---OpenedAt between @StartDate and @EndDate
-- PR.Owning_Team in (select teamId from @teams where fullname like '%' + @CxTeam + '%')

IF OBJECT_ID('tempdb..#TaskScans') IS NOT NULL DROP TABLE #TaskScans

CREATE TABLE #TaskScans (ScanID bigint, ProjectId bigint, ResultId bigint, IsPublic bit, [Owner] nvarchar(100), ScanCompletedOn datetime, RowNumber int)

IF OBJECT_ID('tempdb..#ScanRemarks') IS NOT NULL DROP TABLE #ScanRemarks

CREATE TABLE #ScanRemarks (ProjectID bigint, ResultId bigint, PathID int, SimilarityId bigint ,CreatedOn datetime ,ProjectName nvarchar(100), Userdata nvarchar(255), Comment nvarchar(max), Merged nvarchar(max), RowNumber int)

IF OBJECT_ID('tempdb..#ScanHistory') IS NOT NULL DROP TABLE #ScanHistory

CREATE TABLE #ScanHistory (ProjectID bigint, ResultId bigint, PathID int, SimilarityId bigint ,CreatedOn datetime ,ProjectName nvarchar(100), FirstName nvarchar(50), LastName varchar(100), CreatedBy varchar(100), Comment nvarchar(max))

IF OBJECT_ID('tempdb..#allscans') IS NOT NULL DROP TABLE #allscans

CREATE TABLE #allscans(
 RowNumber bigint, projectName nvarchar(max), teamName nvarchar(max), presetName nvarchar(max), CurrentSimilarityId bigint,  PreviousSimilarityId bigint,  NextSimilarityId bigint, 
 resultId bigint, pathId bigint, nodeId bigint, isFalsePositive nvarchar(10), projectId bigint,
 queryId bigint, scanId bigint, [state] nvarchar(100), [status] nvarchar(100), [severityId] int, [severity] nvarchar(100), [group] nvarchar(max), 
 query nvarchar(1024), [lineNo] int, [column] int, [firstLine] nvarchar(1024), [fileName] nvarchar(max), deepLink nvarchar(max), 
 nodeName nvarchar(max), remark varchar(max),scanDate datetime
 )

 IF OBJECT_ID('tempdb..#finalscans') IS NOT NULL DROP TABLE #finalscans

CREATE TABLE #finalscans(
 RowNumber bigint, ScanID bigint, ProjectName nvarchar(255),projectId bigint, team nvarchar(1024),presetName nvarchar(100), query nvarchar(1024), similarityId bigint, resultId bigint, pathId int, nodeId int, isFalsePositive nvarchar(10),StateDesc nvarchar(1024), [Status] nvarchar(100),
 SeverityID int, Severity nvarchar(100), [lineNo] int, [column] int, [fileName] nvarchar(max),deepLink nvarchar(1024), remark varchar(max),firstScan datetime, lastScan datetime, NewScanDate datetime, FixedScanDate datetime, age int, scanCount int, lastScanId bigint, NotExploitableUser nvarchar(100), NotExploitableDate datetime, SeverityChangeName nvarchar(100), SeverityChangeDate datetime)
 
 IF OBJECT_ID('tempdb..#selectscans') IS NOT NULL DROP TABLE #selectscans

CREATE TABLE #selectscans(
 RowNumber bigint, similarityId bigint, [fileName] nvarchar(max))


  IF OBJECT_ID('tempdb..#completescans') IS NOT NULL DROP TABLE #completescans
CREATE TABLE #completescans(RowNumber bigint, similarityId bigint, [filename] nvarchar(max), projectId bigint)

---Set states per project or team
SELECT @StatePerTeams = (CASE CxComponentConfiguration.Value  WHEN 'true' THEN 1 ELSE 0 END)
FROM CxComponentConfiguration 
WHERE CxComponentConfiguration.[Key]  = 'RESULT_ATTRIBUTES_PER_SIMILARITY';



IF CURSOR_STATUS('global','cxsast_results')>=-1
DEALLOCATE cxsast_results


Declare cxsast_results CURSOR FOR
  Select projectId, projectName, PresetID
  FROM #projects

Open cxsast_results
Fetch NEXT FROM cxsast_results INTO 
    @projectId, 
    @projectName,
	@presetId

While @@FETCH_STATUS = 0
    BEGIN
		Print Cast(@projectID as varchar) + ' - ' + @projectName + ' - ' + Cast(@StartDate as varchar) + ' ' + Cast(@EndDate as varchar)	
				
		Select @TeamString = T.fullname, @Team=T.[teamName] from #teams T where teamId in (Select P.OwningTeamName from #projects P where P.projectId = @ProjectID)
		Select @presetName = presetName From #presets where presetId = @presetId

		Print ' Team=' + @TeamString + ', Preset=' + @presetName

	Delete from #TaskScans

		Insert into #TaskScans
			SELECT 
			Id,
			ProjectId, 
			ResultId, 
			IsPublic,
			[Owner],
			ScanRequestCompletedOn,
			row_number() over (order by TaskScans.ResultId)
			FROM TaskScans WHERE TaskScans.ProjectId = @ProjectId and ScanRequestCompletedOn between @StartDate and @EndDate
		
		SET @id = 1
		Select @scancount = count(ScanId) from #TaskScans
		Print 'Scan Count : ' +  Cast(@scancount as nvarchar(20))

		Delete from #ScanRemarks
		Delete from #ScanHistory
		
		
		
		--CREATE TABLE #ScanHistory (ProjectID bigint, ResultId bigint, PathID int, SimilarityId bigint ,CreatedOn datetime ,ProjectName nvarchar(100), FirstName nvarchar(50), LastName varchar(100), CreatedBy varchar(100), Comment nvarchar(max), DateCreated datetime)


		Insert into #ScanHistory
		SELECT  
		@projectId,
		ResultsLabels.ResultId,
		ResultsLabels.PathID,
		ResultsLabels.SimilarityId ,
		ResultsLabelsHistory.DateCreated,
		ISNULL(Projects.Name,' '), 
		ISNULL(users.FirstName,''),
		ISNULL(users.LastName,''),
		ResultsLabelsHistory.CreatedBy,
		ISNULL(ResultsLabelsHistory.Data, '')
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

		Insert into #ScanRemarks
		SELECT  DISTINCT  
		@projectId,
		@CurrentScanResultID,
		ResultsLabels.PathID,
		ResultsLabels.SimilarityId ,
		ResultsLabelsHistory.DateCreated,
		ISNULL(Projects.Name,' '), 
		ISNULL(users.FirstName + ' ' + users.LastName, ResultsLabelsHistory.CreatedBy),
		ISNULL(ResultsLabelsHistory.Data , ''),
		ISNULL(users.FirstName + ' ' + users.LastName, ResultsLabelsHistory.CreatedBy) + ' ' + @projectname + ',[' +  FORMAT(ResultsLabelsHistory.DateCreated, 'dddd, MMMM, dd, yyyy hh:mm') + ']: ' +  ISNULL(ResultsLabelsHistory.Data , ''),
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

		Delete from #allScans -- Only keep current project scans

		Print ' count=' + Cast(@id as varchar(20)) + ' total=' + Cast(@scancount as varchar(20))
		SELECT @FirstScanId = min(ScanId) from #TaskScans where ProjectId = @ProjectId
		SELECT @LastScanId = max(ScanId) from #TaskScans where ProjectId = @ProjectId
		While (@id <= @scancount)
		BEGIN
			Delete from #GetScanReportResults

			SELECT @ScanID = ScanId, @CurrentScanResultID = ResultId, @ScanVisibility =IsPublic, @Owner=Owner, @ScanCompletedDate = ScanCompletedOn FROM #TaskScans WHERE ProjectID = @ProjectId and RowNumber = @id
			AND ScanCompletedOn between @StartDate and @EndDate

			--Print 'ScanId : ' +  Cast(@ScanId as nvarchar(20))
					
		   --Get previous scan ------------------------------------------------------------------------------------		
			SELECT  @PreviousScanResultID = ISNULL(MAX(PreviousScan.ResultId) , -1) 
			FROM TaskScans AS PreviousScan
			WHERE PreviousScan.ProjectId = @ProjectID
				AND PreviousScan.Id < @ScanID 
				AND PreviousScan.ScanType = 1  /*Regular scan*/
				AND PreviousScan.is_deprecated = 0
				AND (PreviousScan.IsPublic =1 OR (@ScanVisibility=0 AND @Owner = PreviousScan.Owner));
			--------------------------------------------------------------------------------------------------------
		    --Get next scan ------------------------------------------------------------------------------------		
			SELECT  @NextScanResultID = ISNULL(MAX(NextScan.ResultId) , -1) 
			FROM TaskScans AS NextScan
			WHERE NextScan.ProjectId = @ProjectID
				AND NextScan.Id > @ScanID 
				AND NextScan.ScanType = 1  /*Regular scan*/
				AND NextScan.is_deprecated = 0
				AND (NextScan.IsPublic =1 OR (@ScanVisibility=0 AND @Owner = NextScan.Owner));
			--------------------------------------------------------------------------------------------------------

			---Retrieve latest result labels
			INSERT INTO #ScanResultsLabels

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

			INSERT INTO #GetScanReportResults
				SELECT  DISTINCT
							CurrentScanPathResults.QueryVersionCode
							,ResultQuery.Severity AS  QuerySeverity
							,CurrentScanPathResults.Path_Id 
							,CurrentScanPathResults.Similarity_Hash
							,PreviousScanPathResults.Similarity_Hash
							,NextScanPathResults.Similarity_Hash
							, CASE 
								WHEN isnull(PreviousScanPathResults.Similarity_Hash,0) = 0 and @ScanID <> @firstScanId THEN @NewStatus 
								WHEN isnull(NextScanPathResults.Similarity_Hash,0) = 0 AND @ScanId <> @lastScanId THEN @Fixed
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
					LEFT OUTER JOIN (SELECT p.Similarity_Hash , p.QueryVersionCode 
										FROM PathResults AS P 
										WHERE p.ResultId = @NextScanResultID GROUP BY p.Similarity_Hash , p.QueryVersionCode
										) AS NextScanPathResults 
										ON NextScanPathResults.Similarity_Hash = CurrentScanPathResults.Similarity_Hash 
										AND NextScanPathResults.Similarity_Hash <> 0                    
										AND NextScanPathResults.QueryVersionCode = CurrentScanPathResults.QueryVersionCode
					LEFT OUTER JOIN #ScanResultsLabels AS ResultLabelsSeverity ON ResultLabelsSeverity.SimilarityID  =   CurrentScanPathResults.Similarity_Hash   AND ResultLabelsSeverity.LabelType = 2 AND ResultLabelsSeverity.RowNumber = 1
					LEFT OUTER JOIN #ScanResultsLabels AS ResultLabelsState ON ResultLabelsState.SimilarityID  =   CurrentScanPathResults.Similarity_Hash   AND ResultLabelsState.LabelType = 3 AND ResultLabelsState.RowNumber  = 1            
					LEFT OUTER JOIN #ScanResultsLabels AS ResultLabelsAssignedTo ON ResultLabelsAssignedTo.SimilarityID  =   CurrentScanPathResults.Similarity_Hash   AND ResultLabelsAssignedTo.LabelType = 4 AND ResultLabelsAssignedTo.RowNumber = 1           
				WHERE CurrentScanPathResults.ResultId = @CurrentScanResultID 
					AND CurrentScanPathResults.Similarity_Hash <> 0 

			--Select * from @GetScanReportResults where Similarity_Hash = 2120729509

			Insert Into #allScans

			SELECT 
						row_number() over (order by CurrentScanPathResults.Path_Id, [NodeResults].Node_Id asc)
						,@projectName
						,@TeamString
						,@presetName
						,CurrentScanPathResults.CurrentSimilarity_Hash
						,CurrentScanPathResults.PreviousSimilarity_Hash
						,CurrentScanPathResults.NextSimilarity_Hash
						,@CurrentScanResultID
						,CurrentScanPathResults.Path_Id
						,[NodeResults].Node_Id
						,(Case When CurrentScanPathResults.ResultState = 1 then 'TRUE' else 'FALSE' end)
						,@projectId
						,QueryVersion.QueryId
						,@ScanId
						,ResultState.[Name]
						,CurrentScanPathResults.ResultStatus
						,CurrentScanPathResults.ResultSeverity
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
								 FROM #ScanRemarks SR WHERE SR.SimilarityId=CurrentScanPathResults.CurrentSimilarity_Hash ORDER BY SR.RowNumber FOR XML PATH (''))
						,@ScanCompletedDate
			FROM     #GetScanReportResults AS CurrentScanPathResults 
				JOIN NodeResults ON NodeResults.ResultId = @CurrentScanResultID AND NodeResults.Path_Id = CurrentScanPathResults.Path_Id
				JOIN QueryVersion ON QueryVersion.QueryVersionCode = CurrentScanPathResults.QueryVersionId
				JOIN Query ON Query.QueryId = QueryVersion.QueryId
				JOIN QueryGroup ON QueryGroup.PackageId = Query.PackageId
				JOIN ResultState ON ResultState.Id = CurrentScanPathResults.ResultState and ResultState.LanguageId = 1033
				WHERE [Node_Id] = (Case when @TopNode = 1 then 1 else [Node_Id] end)


		SET @id = @id + 1
	END


		Select @firstScanID = min(ScanID) from #allscans 
		Select @lastScanID = max(ScanID) from #allscans 

		Select Top 1 @firstFoundScan = ScanDate from #allscans where ScanId = @firstScanId
		Select Top 1 @lastFoundScan = ScanDate from #allscans where ScanId = @LastScanId

	
		Delete from #selectscans
		Insert into #selectscans
			Select row_number() over (order by ascan.CurrentSimilarityId asc) as RowNumber,
			ascan.CurrentSimilarityId, 
			ascan.[fileName] from (select distinct CurrentSimilarityId, [filename] from #allscans) as ascan

		Select @vulerabilityCount = max(RowNumber) from #selectscans

		SET @id = 1

		While (@id <= @vulerabilityCount)
		BEGIN
			SET @firstSingularityId = 0
			SET @resultStatus = ''
			SET @NewScanID = 0
			SET @NotExploitableScanID = 0
			SET @RecurringScanID = 0
			SET @FixedScanID = 0
			-- Get the unique vulerability

			Select @keySID = scan.similarityId, @keyFile = scan.fileName from #selectscans scan where RowNumber = @id

			Select Top 1 @firstSingularityId = isnull(allScans.CurrentsimilarityId,0), @firstFileName = allScans.[fileName]	
			from #allscans allScans where allscans.CurrentSimilarityId = @keySID and allscans.fileName = @keyFile
			order by allscans.scanDate asc

			-- Now see if we did this yet
			
			if not exists(Select similarityid from #completescans where [filename] = @firstFileName and similarityId = @firstSingularityId and projectId = @projectId)
				BEGIN -- if not, lets find what we need 

					Select Top 1 @NewScanId = isnull(allscans.scanId,0) from #allscans allscans 
					where allscans.[status] = @NewStatus and allScans.[fileName] = @firstFileName and allscans.CurrentsimilarityId = @firstSingularityId and allscans.projectId = @projectId
					order by allscans.scanDate desc	

					Select Top 1 @FixedScanId = isnull(allscans.scanId,0) from #allscans allscans 
					where allscans.[status] = @Fixed and allScans.[fileName] = @firstFileName and allscans.CurrentsimilarityId = @firstSingularityId and allscans.projectId = @projectId
					order by allscans.scanDate desc

					Select Top 1 @NotExploitableScanID = isnull(allscans.scanId,0) from #allscans allscans 
					where allscans.isFalsePositive = 'TRUE' and allScans.[fileName] = @firstFileName and allscans.CurrentsimilarityId = @firstSingularityId and allscans.projectId = @projectId
					order by allscans.scanDate desc					
										
					if (isnull(@NewScanID,0) > isnull(@FixedScanId,0)) -- Issue came back
						SET @fixedScanID = null
					
						--if exists(select * from #ScanHistory AS sh  where sh.SimilarityId = @firstSingularityId)
						--BEGIN			
						--	SELECT top 1 sh.Comment, sh.CreatedBy, sh.CreatedOn, sh.SimilarityId, sh.Resultid FROM #ScanHistory AS sh 
					 --       where sh.SimilarityId = @firstSingularityId	and Comment like '%Not Exploitable%'		
						--	order by sh.CreatedOn desc

						--	SELECT top 1 sh.Comment, sh.CreatedBy, sh.CreatedOn, sh.SimilarityId, sh.Resultid FROM #ScanHistory AS sh 
					 --       where sh.SimilarityId = @firstSingularityId	and Comment like '%severity%'		
						--	order by sh.CreatedOn desc
						--END

					BEGIN TRY			
						if @NotExploitableScanId != 0
							BEGIN
								if exists(Select scanid from #allscans where [filename] = @firstFileName and CurrentSimilarityId = @firstSingularityId and scanid = @NotExploitableScanId)
									Insert into #finalscans
									Select 
										@id as [row_number],
										lastScan.scanId, lastScan.projectName, lastScan.projectId,lastScan.teamName,@presetName,lastScan.query,lastScan.CurrentSimilarityId,
										lastScan.resultId,lastScan.pathId,lastScan.nodeId,lastScan.isFalsePositive,lastScan.[state],lastScan.[status],lastScan.severityId,
										lastScan.[severity],lastScan.[lineNo],lastScan.[column],lastScan.[fileName],lastScan.deepLink,lastScan.remark,@firstFoundScan,
										@LastFoundScan,null,null,0,@scancount,@lastScanID
  									    ,(SELECT Top 1 sh.CreatedBy FROM #ScanHistory AS sh where sh.Comment like '%Not Exploitable%' and sh.ResultId = @NotExploitableScanId and sh.SimilarityId = @firstSingularityId  order by sh.CreatedOn desc)
  									    ,(SELECT Top 1 sh.CreatedOn FROM #ScanHistory AS sh where sh.Comment like '%Not Exploitable%' and sh.ResultId = @NotExploitableScanId and sh.SimilarityId = @firstSingularityId  order by sh.CreatedOn desc)
  									    ,(SELECT Top 1 sh.CreatedBy FROM #ScanHistory AS sh where sh.Comment like '%severity%' and sh.ResultId = @NotExploitableScanId and sh.SimilarityId = @firstSingularityId  order by sh.CreatedOn desc)
  									    ,(SELECT Top 1 sh.CreatedOn FROM #ScanHistory AS sh where sh.Comment like '%severity%' and sh.ResultId = @NotExploitableScanId and sh.SimilarityId = @firstSingularityId  order by sh.CreatedOn desc)
									 From #allScans lastScan
										where [filename] = @firstFileName and CurrentSimilarityId = @firstSingularityId and scanid = @NotExploitableScanId
								else 
									Print 'Error: ' + Cast(@projectId as varchar(100)) + ' ' + Cast(@NotExploitableScanId as varchar(100)) + ' ' + Cast(@firstSingularityId as varchar(100)) + ' ' + Cast(@firstSingularityId as varchar(100))

							END
						 else if @FixedScanId != 0
							BEGIN
								if exists(Select scanid from #allscans where [filename] = @firstFileName and CurrentSimilarityId = @firstSingularityId and scanid = @FixedScanId)
							Insert into #finalscans
								Select 
									@id as [row_number],
									lastScan.scanId, lastScan.projectName, lastScan.projectId,lastScan.teamName,@presetName,lastScan.query,lastScan.CurrentSimilarityId,
									lastScan.resultId,lastScan.pathId,lastScan.nodeId,lastScan.isFalsePositive,lastScan.[state],lastScan.[status],lastScan.severityId,
									lastScan.[severity],lastScan.[lineNo],lastScan.[column],lastScan.[fileName],lastScan.deepLink,lastScan.remark,@firstFoundScan,
									@LastFoundScan,null,lastscan.scanDate, DateDiff(day,lastScan.scanDate,@isToday),@scancount,@lastScanId
 									,(SELECT Top 1 sh.CreatedBy FROM #ScanHistory AS sh where sh.Comment like '%Not Exploitable%' and sh.ResultId = @FixedScanId and sh.SimilarityId = @firstSingularityId  order by sh.CreatedOn desc)
  									,(SELECT Top 1 sh.CreatedOn FROM #ScanHistory AS sh where sh.Comment like '%Not Exploitable%' and sh.ResultId = @FixedScanId and sh.SimilarityId = @firstSingularityId  order by sh.CreatedOn desc)
  									,(SELECT Top 1 sh.CreatedBy FROM #ScanHistory AS sh where sh.Comment like '%severity%' and sh.ResultId = @FixedScanId and sh.SimilarityId = @firstSingularityId  order by sh.CreatedOn desc)
  									,(SELECT Top 1 sh.CreatedOn FROM #ScanHistory AS sh where sh.Comment like '%severity%' and sh.ResultId = @FixedScanId and sh.SimilarityId = @firstSingularityId  order by sh.CreatedOn desc)
								 From #allScans lastScan
									where [filename] = @firstFileName and CurrentSimilarityId = @firstSingularityId and scanid = @FixedScanId
								else 
									Print 'Error: ' + Cast(@projectId as varchar(100)) + ' ' + Cast(@FixedScanId as varchar(100)) + ' ' + Cast(@firstSingularityId as varchar(100)) + ' ' + Cast(@firstSingularityId as varchar(100))

							END

						else if @NewScanId != 0
							BEGIN
								if exists(Select scanid from #allscans where [filename] = @firstFileName and CurrentSimilarityId = @firstSingularityId and scanid = @NewScanID)
							Insert into #finalscans
								Select 
									@id as [row_number],
									lastScan.scanId, lastScan.projectName, lastScan.projectId,lastScan.teamName,@presetName,lastScan.query,lastScan.CurrentSimilarityId,
									lastScan.resultId,lastScan.pathId,lastScan.nodeId,lastScan.isFalsePositive,lastScan.[state],lastScan.[status],lastScan.severityId,
									lastScan.[severity],lastScan.[lineNo],lastScan.[column],lastScan.[fileName],lastScan.deepLink,lastScan.remark,@firstFoundScan,
								@LastFoundScan,lastscan.scanDate,null, DateDiff(day,lastScan.scanDate,@isToday),@scancount,@lastScanId
 									,(SELECT Top 1 sh.CreatedBy FROM #ScanHistory AS sh where sh.Comment like '%Not Exploitable%' and sh.ResultId = @NewScanId and sh.SimilarityId = @firstSingularityId  order by sh.CreatedOn desc)
  									,(SELECT Top 1 sh.CreatedOn FROM #ScanHistory AS sh where sh.Comment like '%Not Exploitable%' and sh.ResultId = @NewScanId and sh.SimilarityId = @firstSingularityId  order by sh.CreatedOn desc)
  									,(SELECT Top 1 sh.CreatedBy FROM #ScanHistory AS sh where sh.Comment like '%severity%' and sh.ResultId = @NewScanId and sh.SimilarityId = @firstSingularityId  order by sh.CreatedOn desc)
  									,(SELECT Top 1 sh.CreatedOn FROM #ScanHistory AS sh where sh.Comment like '%severity%' and sh.ResultId = @NewScanId and sh.SimilarityId = @firstSingularityId  order by sh.CreatedOn desc)
								 From #allScans lastScan
									 where [filename] = @firstFileName and CurrentSimilarityId = @firstSingularityId and scanid = @NewScanId			
								else 
									Print 'Error: ' + Cast(@projectId as varchar(100)) + ' ' + Cast(@NewScanId as varchar(100)) + ' ' + Cast(@firstSingularityId as varchar(100)) + ' ' + Cast(@firstSingularityId as varchar(100))

							END

						 else if @RecurringScanId != 0
							BEGIN
								if exists(Select scanid from #allscans where [filename] = @firstFileName and CurrentSimilarityId = @firstSingularityId and scanid = @RecurringScanId)
							Insert into #finalscans
								Select 
									@id as [row_number],
									lastScan.scanId, lastScan.projectName, lastScan.projectId,lastScan.teamName,@presetName,lastScan.query,lastScan.CurrentSimilarityId,
									lastScan.resultId,lastScan.pathId,lastScan.nodeId,lastScan.isFalsePositive,lastScan.[state],lastScan.[status],lastScan.severityId,
									lastScan.[severity],lastScan.[lineNo],lastScan.[column],lastScan.[fileName],lastScan.deepLink,lastScan.remark,@firstFoundScan,
									@LastFoundScan,null,null,DateDiff(day,lastScan.scanDate,@isToday),@scancount,@lastScanId
 									,(SELECT Top 1 sh.CreatedBy FROM #ScanHistory AS sh where sh.Comment like '%Not Exploitable%' and sh.ResultId = @RecurringScanId and sh.SimilarityId = @firstSingularityId  order by sh.CreatedOn desc)
  									,(SELECT Top 1 sh.CreatedOn FROM #ScanHistory AS sh where sh.Comment like '%Not Exploitable%' and sh.ResultId = @RecurringScanId and sh.SimilarityId = @firstSingularityId  order by sh.CreatedOn desc)
  									,(SELECT Top 1 sh.CreatedBy FROM #ScanHistory AS sh where sh.Comment like '%severity%' and sh.ResultId = @RecurringScanId and sh.SimilarityId = @firstSingularityId  order by sh.CreatedOn desc)
  									,(SELECT Top 1 sh.CreatedOn FROM #ScanHistory AS sh where sh.Comment like '%severity%' and sh.ResultId = @RecurringScanId and sh.SimilarityId = @firstSingularityId  order by sh.CreatedOn desc)
								 From #allScans lastScan
									 where [filename] = @firstFileName and CurrentSimilarityId = @firstSingularityId and scanid = @RecurringScanId
								else 
									Print 'Error: ' + Cast(@projectId as varchar(100)) + ' ' + Cast(@RecurringScanId as varchar(100)) + ' ' + Cast(@firstSingularityId as varchar(100)) + ' ' + Cast(@firstSingularityId as varchar(100))

							END					  
					        -- Add to skip error
					        Insert into #completescans
							   Select @id as [row_number],@firstSingularityId,@firstFileName,@projectId
				   
					END TRY
				    BEGIN CATCH
						Print 'Error: ' + Cast(@projectId as varchar(100)) + ' ' + Cast(@id as varchar(100)) + ' ' + Cast(@firstSingularityId as varchar(100)) + ' ' + Cast(@firstSingularityId as varchar(100))
						Insert into #completescans
							Select @id as [row_number],@firstSingularityId,@firstFileName,@projectId
					END CATCH		
			    END
			SET @id = @id + 1
		END


		--Select * from @firstscans 
		
		--Select * from @finalScans where similarityId = 2120729509
		--Select * from @allScans where similarityId = 2120729509

		FETCH NEXT FROM cxsast_results INTO 
			@projectId, 
			@projectName,
			@presetId
	END

	
   If isnull(@CxTeam,'') != ''
		Delete from #finalscans where team not like '%' + @CxTeam + '%'

   If isnull(@CxFileExt,'') != ''
		Delete from #finalscans where [Filename] not like '%' + @CxFileExt + '%'

   if (@isSummary != 'true')
	Select 
		firstScans.ProjectName as ProjectName,
		firstScans.team as Team,
		firstScans.ScanID as ScanId,
		firstScans.presetName as PresetName,
		firstScans.Query as QueryName,
		firstScans.isFalsePositive as IsFalsePositive,
		firstScans.StateDesc as StateDesc,
		firstScans.[Status] as [Status],
		firstScans.Severity as Severity,
		firstScans.[LineNo] as [LineNo],
		firstScans.[column] as [column],
		firstScans.similarityId as [SimilarityID],
		firstScans.[fileName] as [Filename],
		firstScans.[deeplink] as [DeepLink],
		firstScans.[remark] as Remark,
		firstScans.[FirstScan] as FirstScanDate,
		firstScans.[LastScan] as LastScanDate,
		firstScans.[NewScanDate] as NewScanDate,
		firstScans.[FixedScanDate] as FixedScanDate,
		firstScans.[age] as Age,
		firstScans.[scanCount] as ScanCount,
	    (select max(ScanId) from #finalscans pid where firstScans.projectName = pid.projectName) as LastScanId,
		firstScans.[NotExploitableUser] as NotExploitableUser,
		firstScans.[NotExploitableDate] as NotExploitableDate,
		firstScans.[SeverityChangeName] as SeverityChangeUser,
		firstScans.[SeverityChangeDate] as SeverityChangeDate
	

		from #finalscans as firstScans
		order by firstscans.ProjectName

	else
		Begin
		Select final.ProjectName, final.ScanID
		,(select count(*) from #finalscans pid where Severity = 'High' and final.projectName = pid.projectName) as High
		,(select count(*) from #finalscans pid where Severity = 'Medium' and final.projectName = pid.projectName) as Medium
		,(select count(*) from #finalscans pid where Severity = 'Low' and final.projectName = pid.projectName) as Low
		,(select count(*) from #finalscans pid where Severity = 'Info' and final.projectName = pid.projectName) as Info
		,(select count(*) from #finalscans pid where isFalsePositive = 'TRUE' and final.projectName = pid.projectName) as FalsePositive
		,(select count(*) from #finalscans pid where StateDesc = 'To Verify' and final.projectName = pid.projectName) as ToVerify
		,(select count(*) from #finalscans pid where StateDesc = 'Not Exploitable' and final.projectName = pid.projectName) as NotExploitable
		,(select count(*) from #finalscans pid where StateDesc = 'Confirm' and final.projectName = pid.projectName) as Confirm
		,(select count(*) from #finalscans pid where StateDesc = 'Urgent' and final.projectName = pid.projectName) as Urgent
		,(select count(*) from #finalscans pid where StateDesc = 'Proposed Not Exploitable' and final.projectName = pid.projectName) as ProposedNotExploitable

		from #finalscans final
		group by final.ProjectName,final.ScanId
		order by ProjectName


		Select final.ProjectName, final.ScanID
		,(select count(*) from #finalscans pid where Severity = 'High' and final.projectName = pid.projectName) as High
		,(select count(*) from #finalscans pid where Severity = 'Medium' and final.projectName = pid.projectName) as Medium
		,(select count(*) from #finalscans pid where Severity = 'Low' and final.projectName = pid.projectName) as Low
		,(select count(*) from #finalscans pid where Severity = 'Info' and final.projectName = pid.projectName) as Info
		,(select count(*) from #finalscans pid where isFalsePositive = 'TRUE' and final.projectName = pid.projectName) as FalsePositive
		,(select count(*) from #finalscans pid where StateDesc = 'To Verify' and final.projectName = pid.projectName) as ToVerify
		,(select count(*) from #finalscans pid where StateDesc = 'Not Exploitable' and final.projectName = pid.projectName) as NotExploitable
		,(select count(*) from #finalscans pid where StateDesc = 'Confirm' and final.projectName = pid.projectName) as Confirm
		,(select count(*) from #finalscans pid where StateDesc = 'Urgent' and final.projectName = pid.projectName) as Urgent
		,(select count(*) from #finalscans pid where StateDesc = 'Proposed Not Exploitable' and final.projectName = pid.projectName) as ProposedNotExploitable


		from #finalscans final
		where final.scanId = (select max(ScanId) from #finalscans pid where final.projectName = pid.projectName)
		group by final.ProjectName,ScanId
		order by final.ProjectName
		
	END

CLOSE cxsast_results
DEALLOCATE cxsast_results
