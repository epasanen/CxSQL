DECLARE @StartDate datetime = '01-01-2017'
DECLARE @EndDate datetime = '12-31-2021'
DECLARE @projectId int
DECLARE @projectName nvarchar(255)
Declare @scanCount int


DECLARE @teams table (teamId nvarchar(255) ,teamName nvarchar(255), teampath nvarchar(255), fullname nvarchar(255))
Insert Into @teams
 Select T.Id, T.[Name], T.Path, T.FullName from [CxDB].[CxEntities].[Team] T

DECLARE @presets table (presetId bigint ,presetName nvarchar(255))
Insert Into @presets
 Select P.Id, P.Name from [CxDB].[dbo].Presets P

DECLARE @projects table(projectId bigint, projectName nvarchar(255), OwningTeamName int, PresetID int)
Insert into @projects
 Select PR.Id, PR.Name, PR.Owning_Team, PR.PresetId from [CxDB].[dbo].[Projects] PR

DECLARE @firstscan table(Id int, ProjectId bigint, ScanId bigint, StartTime datetime, High int, Medium int, Low int, Information int)
DECLARE @lastscan table(Id int, ProjectId bigint, ScanId bigint, StartTime datetime, High int, Medium int, Low int, Information int)
DECLARE @totalscans table(Id int, ProjectId bigint, ScanId bigint, StartTime datetime, High int, Medium int, Low int, Information int)

DECLARE @Split table([Id] int, [Name] nvarchar(255))

Declare @allscans table(
 ProjectId bigint, ProjectName nvarchar(255), Company nvarchar(255), Team nvarchar(255), 
 StartHigh int, StartMedium int, StartLow int,
 LastHigh int, LastMedium int, LastLow int, 
 DiffHigh int, DiffMedium int, DiffLow int,
 NewHigh int, NewMedium int, NewLow int, 
 NotExploitable int, Confirmed int, ToVerify int, 
 firstScan datetime, lastScan datetime, 
 ScanCount int
 )

IF CURSOR_STATUS('global','cxsast_results')>=-1
DEALLOCATE cxsast_results


DECLARE cxsast_results CURSOR FOR
  SELECT projectId, projectName
  FROM @projects

OPEN cxsast_results
FETCH NEXT FROM cxsast_results INTO 
    @projectId, 
    @projectName

WHILE @@FETCH_STATUS = 0
    BEGIN
		Print Cast(@projectID as varchar) + '-' + @projectName + '-' + Cast(@StartDate as varchar) + ' ' + Cast(@EndDate as varchar)	
		
		Delete from @firstscan
		Delete from @lastscan
		Delete from @totalscans

		Insert into @firstscan
			Select Top 1 0, S.ProjectId,S.Id,S.StartTime,S.High,S.Medium,S.Low,S.Information From [CxDB].[dbo].[TaskScans] S where S.ProjectId = @projectId and S.StartTime between @StartDate and @EndDate order by StartTime asc
		Insert into @lastscan
			Select Top 1 0, S.ProjectId,S.Id,S.StartTime,S.High,S.Medium,S.Low,S.Information From [CxDB].[dbo].[TaskScans] S where S.ProjectId = @projectId and S.StartTime between @StartDate and @EndDate order by StartTime desc	
		Insert into @totalscans
			Select 0, S.ProjectId,S.Id,S.StartTime,S.High,S.Medium,S.Low,S.Information From [CxDB].[dbo].[TaskScans] S where S.ProjectId = @projectId and S.StartTime between @StartDate and @EndDate
		Select @scanCount = count(*) from @totalscans
		
			
		Declare @Company nvarchar(255) 
		Declare @Team nvarchar(255)
		Declare @NotExploitable int
		Declare @Confirmed int
		Declare @ToVerify int
		Declare @NewHigh int
		Declare @NewMedium int
		Declare @NewLow int
		DECLARE @name NVARCHAR(255)
        DECLARE @pos INT
		DECLARE @scount int = 1


		DECLARE @TeamString nvarchar(255)
		Select @TeamString = T.fullname, @Team=T.[teamName] from @teams T where teamId in (Select P.OwningTeamName from @projects P where P.projectId = @ProjectID)
		
		WHILE CHARINDEX('/', @TeamString) > 0
		BEGIN
			SELECT @pos  = CHARINDEX('/', @TeamString)  
			SELECT @name = SUBSTRING(@TeamString, 1, @pos-1)

	
			INSERT INTO @Split 
				SELECT @scount, @name

			SET @scount = @scount + 1
			SELECT @TeamString = SUBSTRING(@TeamString, @pos+1, LEN(@TeamString)-@pos)
		END

		Select @Company = [Name] from @Split where Id = 4
	
		SELECT @NewHigh = isNull(sum(S.[NewResults]),0) FROM [CxDB].[dbo].[ScanStatistics] S
			JOIN [CxDB].[dbo].[Query] Q on Q.QueryId = S.QueryId
			Where S.ScanId in (Select ScanId from @totalscans) and Q.Severity = 3

		SELECT @NewMedium = isNull(sum(S.[NewResults]),0) FROM [CxDB].[dbo].[ScanStatistics] S
			JOIN [CxDB].[dbo].[Query] Q on Q.QueryId = S.QueryId
			Where S.ScanId in (Select ScanId from @totalscans) and Q.Severity = 2

		SELECT @NewLow = isNull(sum(S.[NewResults]),0) FROM [CxDB].[dbo].[ScanStatistics] S
			JOIN [CxDB].[dbo].[Query] Q on Q.QueryId = S.QueryId
			Where S.ScanId in (Select ScanId from @totalscans) and Q.Severity = 1

		SELECT  @NotExploitable = Count(A.Id)
			FROM [CxDB].[CxEntities].Result A
				where scanID = (Select Top 1 scanid from @lastscan) 
				and A.[State] = 1

		SELECT  @Confirmed = Count(A.Id)
			FROM [CxDB].[CxEntities].Result A
				where scanID = (Select Top 1 scanid from @lastscan)
				and A.[State] = 2	

		SELECT  @ToVerify = Count(A.Id)
			FROM [CxDB].[CxEntities].Result A
				where scanID = (Select Top 1 scanid from @lastscan)
				and A.[State] = 0	


		Insert into @allscans 
			Select
			  @projectId, 
			  @projectName, 
			  @Company,
			  @Team, 
			  FS.High, 
			  FS.Medium,
			  FS.Low,
			  LS.High,
			  LS.Medium,
			  LS.Low, 
			  FS.High - LS.High,
			  FS.Medium - LS.Medium,
			  FS.Low - LS.Low,
			  @NewHigh, 
			  @NewMedium,
			  @NewLow, 
			  @NotExploitable,
			  @Confirmed,
			  @ToVerify, 
			  FS.StartTime, 
			  LS.StartTime, 
			  @scanCount
			From @firstscan FS
			JOIN @lastscan LS on FS.ProjectId = LS.ProjectId


		FETCH NEXT FROM cxsast_results INTO 
			@projectId, 
			@projectName
	END

Select * from @allScans

CLOSE cxsast_results
DEALLOCATE cxsast_results
