
USE CxDB

SET  NOCOUNT ON

Declare @StartDate datetime = Convert(datetime, '01-01-2010',101)
Declare @EndDate datetime = Convert(datetime, '12-31-2021', 101)
Declare @CxTeam nvarchar(100) = null
Declare @CxProject nvarchar(100) = 'Webgoat'
Declare @Url nvarchar(max) = 'https://securecodeanalysis04.citigroup.net/CxWebClient/ViewerMain.aspx'
Declare @projectId int
Declare @projectName nvarchar(255)
Declare @presetId int
Declare @scanId int
Declare @pathId int



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
 Select PR.Id, PR.Name, PR.Owning_Team, PR.PresetId, PR.OpenedAt from [CxDB].[dbo].[Projects] PR 

If isnull(@CxTeam,'') != ''
  Delete from #projects where OwningTeamName not in (select teamId from #teams where fullname like '%' + @CxTeam + '%')

If isnull(@CxProject,'') != ''
  Delete from #projects where ProjectName not like '%' + @CxProject + '%'

IF OBJECT_ID('tempdb..#scans') IS NOT NULL DROP TABLE #scans

CREATE TABLE #scans(
 ProjectName nvarchar(255), ProjectID int, ScanId int, PathId int, DeepLink nvarchar(1000),queryName nvarchar(1024), sourceFile nvarchar(1000), SourceLine int, sourceObject nvarchar(100), destinationFile nvarchar(1000), destinationLine int, DestinationObject nvarchar(100),resultState int)


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
		SELECT top 1
		@scanID = ResultId
		FROM TaskScans WHERE TaskScans.ProjectId = @ProjectId and isPublic = 1
		order by ResultId DESC;

		With Node_Range as (
		  SELECT 
		  ResultId,
		  Path_Id,
		  min(Node_Id) as FirstNode,
		  max(Node_Id) as LastNode
		  from [CxDB].[dbo].[NodeResults] nr
		  where ResultID = @scanId
		  group by ResultId,Path_Id
		  )
		  INSERT INTO #scans	  
          SELECT
		    @ProjectName as ProjectName,
			@projectId as ProjectId,
			@scanId as ScanId,
    		nr1.Path_Id as PathId,
			@Url + '?scanid=' + Cast(@scanId as varchar) + '&projectid=' + Cast(@projectId as varchar) + '&pathid=' + Cast(nr1.Path_Id as varchar) as DeepLink,
			qv.[Name],
			nr1.[File_Name] as SourceFile,
			nr1.Line as SourceLine,
			nr1.Short_Name as SourceObject,
			nr2.[File_Name] as DestinationFile,
			nr2.Line as DestinationLine,
			nr2.Short_Name as DestinationObject,
			isnull((select top 1 NumericData from [CxDB].[dbo].[ResultsLabels]  
			  where LabelType = 3 and PathId = nr.Path_Id and ResultId = nr.ResultId and ProjectId = @projectId
			  order by UpdateDate desc),0) as ResultState
			from Node_Range nr
			JOIN [CxDB].[dbo].[PathResults] pr on nr.ResultId = pr.ResultId and nr.Path_Id = pr.Path_Id
			JOIN [CxDB].[dbo].[QueryVersion] qv on qv.QueryVersionCode = pr.QueryVersionCode
			JOIN [CxDB].[dbo].[NodeResults] nr1 on nr.ResultId = nr1.ResultId and nr.Path_Id = nr1.Path_Id and nr1.Node_Id = nr.FirstNode
			JOIN [CxDB].[dbo].[NodeResults] nr2 on nr.ResultId = nr2.ResultId and nr.Path_Id = nr2.Path_Id and nr2.Node_Id = nr.LastNode
			order by pathid
    

		FETCH NEXT FROM cxsast_results INTO 
			@projectId, 
			@projectName,
			@presetId
	END


CLOSE cxsast_results
Select * from #scans
DEALLOCATE cxsast_results
