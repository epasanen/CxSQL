USE [CxDB]
GO

/****** Object:  View [dbo].[v_PathResultsWithLabelData]    Script Date: 10/14/2020 5:10:46 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE VIEW [dbo].[v_PathResultsFirstandLast]
AS

	WITH [Labels_CTE] AS
	(
		SELECT 
			[Labels].[ProjectId],
			[Labels].[TeamId],
			[Labels].[SimilarityId],
			[Labels].[LabelType],
			CAST(NULLIF(SUBSTRING([Labels].[Data], 24, 10), '000000NULL') AS INT) AS [NumericData],
			CAST(NULLIF(SUBSTRING([Labels].[Data], 34, 4000), '__NULL__') AS NVARCHAR(MAX)) AS [StringData]
		FROM 
		(
			SELECT 
				CASE WHEN [SimilarityMode].[PerTeam] = 0 THEN [v_TeamLabels].[ProjectId] ELSE NULL END AS [ProjectId],
				CASE WHEN [SimilarityMode].[PerTeam] = 1 THEN [v_TeamLabels].[TeamId] ELSE NULL END AS [TeamId],
				[v_TeamLabels].[SimilarityId],
				[v_TeamLabels].[LabelType],
				MAX
				(
					(
						CONVERT(CHAR(23), [v_TeamLabels].[UpdateDate], 126)
						+ RIGHT('000000000' + ISNULL(CAST([v_TeamLabels].[NumericData] AS NVARCHAR(10)), 'NULL'), 10)
						+ ISNULL([v_TeamLabels].[StringData], '__NULL__')
					) COLLATE Latin1_General_BIN2
				) AS [Data]
			FROM [dbo].[v_TeamLabels]
			CROSS APPLY 
			(
				SELECT TOP 1 CASE WHEN [Value] = 'true' THEN 1 ELSE 0 END AS [PerTeam]
				FROM [dbo].[CxComponentConfiguration]
				WHERE [Key] = 'RESULT_ATTRIBUTES_PER_SIMILARITY'
			) AS [SimilarityMode]
			WHERE [v_TeamLabels].[LabelType] IN 
				(
					1, -- Comment
					2, -- State
					3, -- Severity
					4  -- Assign To
				)
			GROUP BY 
				CASE WHEN [SimilarityMode].[PerTeam] = 1 THEN [v_TeamLabels].[TeamId] ELSE NULL END,
				CASE WHEN [SimilarityMode].[PerTeam] = 0 THEN [v_TeamLabels].[ProjectId] ELSE NULL END,
				[v_TeamLabels].[SimilarityId],
				[v_TeamLabels].[LabelType]
		) [Labels]
	)
	SELECT 
	          [QueryVersion].[Name] AS [QueryName],
              [Results].[ResultId] AS [ResultId],
              [Results].[Path_Id] AS [Path_Id],
			  [NodeResults].Node_Id as [Node_Id],
              ISNULL([ResultSeverities].[NumericData], [QueryVersion].[Severity]) AS [Severity],
              ISNULL([ResultStates].[NumericData], 0) AS [State],
              [Projects].[Name] AS [Project_Name],
              [Teams].TeamName AS [Team_Name],
              [Results].[Similarity_Hash] AS [SimilarityId],
              [Results].[Date] AS [Date],
			  [NodeResults].[Full_Name] as Full_Name,
			  [NodeResults].[File_Name] as [File_Name],
			  [NodeResults].[Line] as [Line],
			  [NodeResults].[Col] as [Column],
			  [NodeResults].[Length] as [Length],
			  [NodeResults].[DOM_Id] as [DOM_ID],
			  [NodeResults].[Method_Line] as [Method_Line],
              [ResultComments].[StringData] AS [Comment]
	FROM [PathResults] [Results]
	INNER JOIN [TaskScans] [Scans] 
		ON [Results].[ResultId] = [Scans].[ResultId]
	INNER JOIN [Projects] [Projects] 
		ON [Projects].[Id] = [Scans].[ProjectId]
	INNER JOIN [QueryVersion]
		ON [QueryVersion].[QueryVersionCode] = [Results].[QueryVersionCode]
	INNER JOIN [Teams] [Teams] ON [Teams].TeamId = [Projects].[Owning_Team]
	INNER JOIN NodeResults [NodeResults] ON NodeResults.ResultId = Results.ResultId AND NodeResults.Path_Id = Results.Path_Id
	--INNER JOIN TaskScans [PreviousScan] ON PreviousScan.ProjectId = [Scans].ProjectId
	--	AND PreviousScan.Id < [Scans].Id
	--	AND PreviousScan.ScanType = 1  /*Regular scan*/
	--	AND PreviousScan.is_deprecated = 0
	--	AND (PreviousScan.IsPublic =1 OR ([Scans].IsPublic = 0 AND [Scans].[Owner] = [PreviousScan].[Owner]))
	CROSS APPLY
	(
		SELECT TOP 1 CASE WHEN [Value] = 'true' THEN 1 ELSE 0 END AS [PerTeam]
		FROM [dbo].[CxComponentConfiguration]
		WHERE [Key] = 'RESULT_ATTRIBUTES_PER_SIMILARITY'
	) AS [SimilarityMode]
	LEFT JOIN [Labels_CTE] [ResultComments] 
		ON 1 = CASE WHEN [SimilarityMode].[PerTeam] = 1 
			THEN 
				CASE WHEN [ResultComments].[TeamId] = [Projects].[Owning_Team] THEN 1 ELSE 0 END
			ELSE 
				CASE WHEN [ResultComments].[ProjectId] = [Projects].[Id] THEN 1 ELSE 0 END
			END
		AND [ResultComments].[SimilarityId] = [Results].[Similarity_Hash]
		AND [ResultComments].[LabelType] = 1
	LEFT JOIN [Labels_CTE] [ResultSeverities]
		ON 1 = CASE WHEN [SimilarityMode].[PerTeam] = 1 
			THEN 
				CASE WHEN [ResultSeverities].[TeamId] = [Projects].[Owning_Team] THEN 1 ELSE 0 END
			ELSE 
				CASE WHEN [ResultSeverities].[ProjectId] = [Projects].[Id] THEN 1 ELSE 0 END
			END
		AND [ResultSeverities].[SimilarityId] = [Results].[Similarity_Hash]
		AND [ResultSeverities].[LabelType] = 2
	LEFT JOIN [Labels_CTE] [ResultStates]
		ON 1 = CASE WHEN [SimilarityMode].[PerTeam] = 1 
			THEN 
				CASE WHEN [ResultStates].[TeamId] = [Projects].[Owning_Team] THEN 1 ELSE 0 END
			ELSE 
				CASE WHEN [ResultStates].[ProjectId] = [Projects].[Id] THEN 1 ELSE 0 END
			END
		AND [ResultStates].[SimilarityId] = [Results].[Similarity_Hash]
		AND [ResultStates].[LabelType] = 3
	LEFT JOIN [Labels_CTE] [ResultAssignedTo]
		ON 1 = CASE WHEN [SimilarityMode].[PerTeam] = 1 
			THEN 
				CASE WHEN [ResultAssignedTo].[TeamId] = [Projects].[Owning_Team] THEN 1 ELSE 0 END
			ELSE 
				CASE WHEN [ResultAssignedTo].[ProjectId] = [Projects].[Id] THEN 1 ELSE 0 END
			END
		AND [ResultAssignedTo].[SimilarityId] = [Results].[Similarity_Hash]
		AND [ResultAssignedTo].[LabelType] = 4
	LEFT JOIN [Users] 
		ON [Users].[UserName] = [ResultAssignedTo].[StringData] AND [Users].[is_deprecated] = 0
	WHERE [Results].[Similarity_Hash] <> 0
		AND [Scans].[is_deprecated] = 0
		AND [Projects].[is_deprecated] = 0
AND (
	NodeResults.[Node_Id] = (SELECT MIN(Node_Id) FROM NodeResults WHERE Path_Id = Results.Path_Id and ResultId = Results.ResultId) 
OR
    NodeResults.[Node_Id] = (SELECT MAX(Node_Id) FROM NodeResults WHERE Path_Id = Results.Path_Id  and ResultId = Results.ResultId)
)


