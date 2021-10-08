use CxDB
DECLARE @StartDate datetime = '01-01-2017'
DECLARE @EndDate datetime = '12-31-2021'
DECLARE @CxProject nvarchar(100) = null
DECLARE @CxTeam nvarchar(100) = null
DECLARE @CxPreset nvarchar(100) = null
DECLARE @CxFileExt nvarchar(100) = null
DECLARE @projectId int
DECLARE @projectNamed nvarchar(255)
Declare @scanCount int
Declare @presetId bigint
Declare @presetNamed nvarchar(100)


Declare @Severity table(id int, value varchar(20))
Insert into @Severity
	Select 0,'Informational'
Insert into @Severity
	Select 1,'Low'
Insert into @Severity
	Select 2,'Medium'
Insert into @Severity
	Select 3,'High'

SET NOCOUNT ON 
DECLARE @teams table (teamId nvarchar(255) ,teamName nvarchar(255), teampath nvarchar(255), fullname nvarchar(255))
Insert Into @teams
 Select T.Id, T.[Name], T.Path, T.FullName from [CxDB].[CxEntities].[Team] T

DECLARE @presets table (presetId bigint ,presetName nvarchar(255))
Insert Into @presets
 Select P.Id, P.Name from [CxDB].[dbo].Presets P

DECLARE @projects table(projectId bigint, projectName nvarchar(255), OwningTeamName uniqueidentifier, PresetID int)
Insert into @projects
 Select PR.Id, PR.Name, PR.Owning_Team, PR.PresetId from [CxDB].[dbo].[Projects] PR

if (isnull(@CxProject,'') != '')
	DELETE from @projects where projectName not like '%' + @CxProject + '%'


DECLARE @lastscan table(Id int, ProjectId bigint, ScanId bigint, StartTime datetime, High int, Medium int, Low int, Information int)


Declare @allscans table(ProjectName nvarchar(100), Team nvarchar(100), PresetName nvarchar(20), Languages nvarchar(255),LastScanId bigint,
TopQueryName1 nvarchar(50), TopQueryCount_1 int, TopQueryseverity_1 nvarchar(50),
TopQueryName2 nvarchar(50), TopQueryCount_2 int, TopQueryseverity_2 nvarchar(50),
TopQueryName3 nvarchar(50), TopQueryCount_3 int, TopQueryseverity_3 nvarchar(50),
TopQueryName4 nvarchar(50), TopQueryCount_4 int, TopQueryseverity_4 nvarchar(50),
TopQueryName5 nvarchar(50), TopQueryCount_5 int, TopQueryseverity_5 nvarchar(50))


IF CURSOR_STATUS('global','cxsast_results')>=-1
DEALLOCATE cxsast_results


DECLARE cxsast_results CURSOR FOR
  SELECT projectId, projectName, presetID
  FROM @projects

OPEN cxsast_results
FETCH NEXT FROM cxsast_results INTO 
    @projectId, 
    @projectNamed,
	@presetId

WHILE @@FETCH_STATUS = 0
    BEGIN
		Print Cast(@projectID as varchar) + '-' + @projectNamed + '-' + Cast(@StartDate as varchar) + ' ' + Cast(@EndDate as varchar)	
		
		Delete from @lastscan
				
		Declare @Company nvarchar(255) 
		Declare @Team nvarchar(255)


		Select @Team=T.[teamName] from @teams T where teamId in (Select P.OwningTeamName from @projects P where P.projectId = @ProjectID)
		Select @presetNamed = presetName from @presets where presetID = @presetId

  	    Insert into @allscans 
			Select Top 1
				@ProjectNamed,
				@Team,
				@presetNamed,
				(SELECT QL.LanguageName + ',' AS [text()]
					FROM  [CxEntities].[ScanLanguages] SL
					JOIN [dbo].[QueryLanguageStates] QL On SL.LanguageId = QL.Language and SL.VersionId = QL.Id
					WHERE SL.ScanID = TS.Id FOR XML PATH ('')),
				TS.ResultId as LastScanId,
				TS.TopQuery1,
				TS.TopQuery1Count,
				(Select Value from @Severity Where ID = TS.TopQuery1Severity),
				TS.TopQuery2,
				TS.TopQuery2Count,
				(Select Value from @Severity Where ID = TS.TopQuery2Severity),
				TS.TopQuery3,
				TS.TopQuery3Count,
				(Select Value from @Severity Where ID = TS.TopQuery3Severity),
				TS.TopQuery4,
				TS.TopQuery4Count,
				(Select Value from @Severity Where ID = TS.TopQuery4Severity),
				TS.TopQuery5,
				TS.TopQuery5Count,
				(Select Value from @Severity Where ID = TS.TopQuery5Severity)
			From [TaskScans] TS 
			Where TS.ProjectId = @projectId and StartTime between @StartDate and @EndDate
			order by TS.ResultId desc

		FETCH NEXT FROM cxsast_results INTO 
			@projectId, 
			@projectNamed,
			@presetid
	END

	
if (isnull(@CxTeam,'') != '')
	DELETE from @teams where teamName not like '%' + @teamName + '%'

if (isnull(@presetName,'') != '')
	DELETE from @presets where presetName not like '%' + presetName + '%'

Select * from @allScans

CLOSE cxsast_results
DEALLOCATE cxsast_results