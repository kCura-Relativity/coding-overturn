using System;
using System.Collections.Generic;
using System.Data;
using kCura.EventHandler;
using Relativity.API;

namespace CodingOverturnSolution
{
  [kCura.EventHandler.CustomAttributes.Description("Coding Overturn Pre-Save Event Handler")]
  [System.Runtime.InteropServices.Guid("7B145E34-9643-45EC-B308-4E6F496CA749")]
  public class PreSaveEventHandler : kCura.EventHandler.PreSaveEventHandler
  {
    #region "Field Types"
    private const int FtSingleChoice = 5;
    private const int FtMultipleChoice = 8;
    private const int FtYesNo = 3;
    private const string TblFieldsToMonitor = "FieldsToMonitor";
    private const string TblOverturnHistory = "OverturnHistory";
    #endregion

    #region "Production Values"
    private DateTime _currentDateTime;
    #endregion

    public override Response Execute()
    {
      var retVal = new Response
      {
        Success = true,
        Message = string.Empty
      };

      try
      {
        VerifyTempTablesExists();

        var dbWorkspaceConnection = (DBContext)Helper.GetDBContext(Application.ArtifactID);
        var sqlFieldTracking = $@"SELECT FieldArtifactID, group1ArtifactId, group2ArtifactId FROM {TblFieldsToMonitor} WITH(NOLOCK)";
        var dsFields = dbWorkspaceConnection.ExecuteSqlStatementAsDataSet(sqlFieldTracking);
        _currentDateTime = Convert.ToDateTime(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"));

        if (dsFields != null && dsFields.Tables.Count == 1)
        {
          if (dsFields.Tables[0].Rows.Count > 0)
          {
            var colField = ActiveArtifact.Fields;

            foreach (DataRow drFields in dsFields.Tables[0].Rows)
            {
              if ((drFields["FieldArtifactID"] != DBNull.Value))
              {
                foreach (Field field in colField)
                {
                  if (field.ArtifactID == (int)drFields["FieldArtifactID"])
                  {
                    //validate field value, based on field type
                    switch (field.FieldTypeID)
                    {
                      case FtYesNo:
                        if (HasChangedNonChoiceField(ActiveArtifact.ArtifactID, field.ArtifactID, field.FieldTypeID))
                        {
                          var returnValue = SetOverturnHistoryValuesNonChoice(field, (int)drFields["group1ArtifactId"], (int)drFields["group2ArtifactId"]);
                          if (returnValue.Trim().Length > 0)
                          {
                            retVal.Success = false;
                            retVal.Message = returnValue;
                          }
                        }
                        break;
                      case FtSingleChoice:
                      case FtMultipleChoice:
                        if (HasFieldChanged(ActiveArtifact.ArtifactID, field.ArtifactID, field.CodeTypeID))
                        {
                          string returnValue = SetOverturnHistoryValuesChoice(field, (int)drFields["group1ArtifactId"], (int)drFields["group2ArtifactId"]);
                          if (returnValue.Trim().Length > 0)
                          {
                            retVal.Success = false;
                            retVal.Message = returnValue;
                          }
                        }
                        break;
                    }
                  }
                }
              }
            }
          }
        }
      }
      catch (Exception ex)
      {
        retVal.Success = false;
        retVal.Message = ex.ToString();
      }

      return retVal;
    }

    private string SetOverturnHistoryValuesChoice(Field field, int group1ArtifactId, int group2ArtifactId)
    {
      try
      {
        var updateStatus = string.Empty;
        string sqlUpdateTempTable;
        var currentChoiceCollection = ((ChoiceFieldValue)ActiveArtifact.Fields[field.ArtifactID].Value).Choices;
        var userArtifactId = GetCaseUserArtifactId();
        var caseUserGroupTemp = GetCaseUserGroupArtifactId((int)userArtifactId, group1ArtifactId, group2ArtifactId);
        string caseUserGroup;
        var dbWorkspaceConnection = (DBContext)Helper.GetDBContext(Application.ArtifactID);

        if (caseUserGroupTemp.HasValue)
        {
          caseUserGroup = Convert.ToString(caseUserGroupTemp);
        }
        else
        {
          caseUserGroup = "NULL";
        }

        //Check to see if any values currently exist in db
        var sqlCheckCurrentDbChoices = $@"SELECT COUNT(CodeArtifactID) FROM [EDDSDBO].[{"ZCodeArtifact"}_{field.CodeTypeID}] WITH(NOLOCK) WHERE AssociatedArtifactID = {ActiveArtifact.ArtifactID}";
        var dbValues = dbWorkspaceConnection.ExecuteSqlStatementAsScalar(sqlCheckCurrentDbChoices);

        if (currentChoiceCollection.Count > 0)
        {
          foreach (Choice c in currentChoiceCollection)
          {
            sqlUpdateTempTable = $@"INSERT INTO {TblOverturnHistory} (UserArtifactID, GroupArtifactID, DocumentArtifactID, FieldArtifactID, FieldTypeID, FieldValue, CodeArtifactID, [TimeStamp]) SELECT {userArtifactId}, {caseUserGroup}, {ActiveArtifact.ArtifactID}, '{field.ArtifactID}', {field.FieldTypeID}, {"NULL"}, {c.ArtifactID}, '{_currentDateTime}'";
            dbWorkspaceConnection.ExecuteSqlStatementAsScalar(sqlUpdateTempTable);
          }
        }
        else
        {
          if ((int)dbValues > 0)
          {
            sqlUpdateTempTable = $@"INSERT INTO {TblOverturnHistory} (UserArtifactID, GroupArtifactID, DocumentArtifactID, FieldArtifactID, FieldTypeID, FieldValue, CodeArtifactID, [TimeStamp]) SELECT {userArtifactId}, {caseUserGroup}, {ActiveArtifact.ArtifactID}, '{field.ArtifactID}', {field.FieldTypeID}, {"NULL"}, {"NULL"}, '{_currentDateTime}'";
            dbWorkspaceConnection.ExecuteSqlStatementAsScalar(sqlUpdateTempTable);
          }
        }

        return updateStatus;
      }
      catch (Exception ex)
      {
        return $@"Error encountered: {ex}";
      }
    }

    private string SetOverturnHistoryValuesNonChoice(Field field, int group1ArtifactId, int group2ArtifactId)
    {
      try
      {
        var updateStatus = string.Empty;
        var userArtifactId = GetCaseUserArtifactId();
        var dbWorkspaceConnection = (DBContext)Helper.GetDBContext(Application.ArtifactID);

        if (userArtifactId != null)
        {
          var tempBoolean = (bool?)field.Value.Value;
          string sqlUpdateTempTable;
          var caseUserGroupTemp = GetCaseUserGroupArtifactId((int)userArtifactId, group1ArtifactId, group2ArtifactId);
          int? caseUserGroup;

          if (caseUserGroupTemp.HasValue)
          {
            caseUserGroup = caseUserGroupTemp;
          }
          else
          {
            caseUserGroup = null;
          }

          if (tempBoolean.HasValue)
          {
            if ((bool)tempBoolean)
            {
              sqlUpdateTempTable = $@"INSERT INTO [EDDSDBO].[{TblOverturnHistory}] (UserArtifactID, GroupArtifactID, DocumentArtifactID, FieldArtifactID, FieldTypeID, FieldValue, CodeArtifactID, [TimeStamp]) SELECT {userArtifactId}, {caseUserGroup}, {ActiveArtifact.ArtifactID}, '{field.ArtifactID}', {field.FieldTypeID}, {"'True'"}, {"NULL"}, '{_currentDateTime}'";
            }
            else
            {
              sqlUpdateTempTable = $@"INSERT INTO [EDDSDBO].[{TblOverturnHistory}] (UserArtifactID, GroupArtifactID, DocumentArtifactID, FieldArtifactID, FieldTypeID, FieldValue, CodeArtifactID, [TimeStamp]) SELECT {userArtifactId}, {caseUserGroup}, {ActiveArtifact.ArtifactID}, '{field.ArtifactID}', {field.FieldTypeID}, {"'False'"}, {"NULL"}, '{_currentDateTime}'";
            }
          }
          else
          {
            sqlUpdateTempTable = $@"INSERT INTO [EDDSDBO].[{TblOverturnHistory}] (UserArtifactID, GroupArtifactID, DocumentArtifactID, FieldArtifactID, FieldTypeID, FieldValue, CodeArtifactID, [TimeStamp]) SELECT {userArtifactId}, {caseUserGroup}, {ActiveArtifact.ArtifactID}, '{field.ArtifactID}', {field.FieldTypeID}, {"NULL"}, {"NULL"}, '{_currentDateTime}'";
          }

          dbWorkspaceConnection.ExecuteSqlStatementAsScalar(sqlUpdateTempTable);
        }

        return updateStatus;
      }
      catch (Exception ex)
      {
        return $@"Error encountered: {ex}";
      }
    }

    private int? GetCaseUserArtifactId()
    {
      var dbEddsConnection = (DBContext)Helper.GetDBContext(-1);
      var sqlGetCaseUser = $@"SELECT CaseUserArtifactID FROM [EDDSDBO].[UserCaseUser] WITH(NOLOCK) WHERE CaseArtifactID = {Application.ArtifactID} AND UserArtifactID = {Helper.GetAuthenticationManager().UserInfo.ArtifactID}";
      var dsCaseUser = dbEddsConnection.ExecuteSqlStatementAsDataSet(sqlGetCaseUser);

      if (dsCaseUser != null && dsCaseUser.Tables.Count == 1)
      {
        if (dsCaseUser.Tables[0].Rows.Count > 0)
        {
          var drCaseUser = dsCaseUser.Tables[0].Rows[0];
          if (drCaseUser["CaseUserArtifactID"] != DBNull.Value)
          {
            return (int)drCaseUser["CaseUserArtifactID"];
          }
          return null;
        }
        return null;
      }
      return null;
    }

    private int? GetCaseUserGroupArtifactId(int caseUserArtifactId, int group1ArtifactId, int group2ArtifactId)
    {
      var dbWorkspaceConnection = (DBContext)Helper.GetDBContext(Application.ArtifactID);
      var sqlGetCaseUserGroup1 = $@"SELECT COUNT(GroupArtifactID) [GroupArtifactID] FROM [EDDSDBO].[GroupUser] WITH(NOLOCK) WHERE UserArtifactID = {caseUserArtifactId} AND GroupArtifactID = {group1ArtifactId}";
      var sqlGetCaseUserGroup2 = $@"SELECT COUNT(GroupArtifactID) [GroupArtifactID] FROM [EDDSDBO].[GroupUser] WITH(NOLOCK) WHERE UserArtifactID = {caseUserArtifactId} AND GroupArtifactID = {group2ArtifactId}";
      var caseUserGroup1Count = dbWorkspaceConnection.ExecuteSqlStatementAsScalar(sqlGetCaseUserGroup1);
      var caseUserGroup2Count = dbWorkspaceConnection.ExecuteSqlStatementAsScalar(sqlGetCaseUserGroup2);

      if ((int)caseUserGroup1Count > 0)
      {
        if ((int)caseUserGroup2Count > 0)
        {
          return group2ArtifactId;
        }
        else
        {
          return group1ArtifactId;
        }
      }
      else
      {
        if ((int)caseUserGroup2Count > 0)
        {
          return group2ArtifactId;
        }
        else
        {
          return null;
        }
      }
    }

    private void VerifyTempTablesExists()
    {
      var dbWorkspaceConnection = (DBContext)Helper.GetDBContext(Application.ArtifactID);
      var sqlFieldsToMonitor = string.Format("IF OBJECT_ID('{0}', 'U') IS NULL BEGIN CREATE TABLE EDDSDBO.[{0}] (FieldArtifactID INT, group1ArtifactId INT, group2ArtifactId INT) END", TblFieldsToMonitor);
      dbWorkspaceConnection.ExecuteNonQuerySQLStatement(sqlFieldsToMonitor);

      var sqlOverturnHistory = string.Format("IF OBJECT_ID('{0}', 'U') IS NULL BEGIN CREATE TABLE EDDSDBO.[{0}] (UserArtifactID INT, GroupArtifactID INT, DocumentArtifactID INT, FieldArtifactID INT, FieldTypeID INT, FieldValue VARCHAR(MAX), CodeArtifactID INT, [Timestamp] DateTime) END", TblOverturnHistory);
      dbWorkspaceConnection.ExecuteNonQuerySQLStatement(sqlOverturnHistory);
    }

    private bool HasFieldChanged(int documentArtifactId, int fieldArtifactId, int? codeTypeId)
    {
      var fieldChanged = false;
      var previousChoiceValue = new List<int>();
      var currentChoiceValue = new List<int>();
      var dbWorkspaceConnection = (DBContext)Helper.GetDBContext(Application.ArtifactID);

      //Get sorted previous choices in order to compare to current
      string sql = $@"SELECT CodeArtifactId FROM ZCodeArtifact_{codeTypeId} CA WITH(NOLOCK) WHERE AssociatedArtifactID = {documentArtifactId}";
      var ds = dbWorkspaceConnection.ExecuteSqlStatementAsDataSet(sql);

      if (ds != null && ds.Tables.Count == 1)
      {
        foreach (DataRow dr in ds.Tables[0].Rows)
        {
          previousChoiceValue.Add((int)dr["CodeArtifactID"]);
        }
      }

      //Get sorted current choices in order to compare to previos
      var currentChoiceCollection = ((ChoiceFieldValue)ActiveArtifact.Fields[fieldArtifactId].Value).Choices;
      foreach (Choice c in currentChoiceCollection)
      {
        currentChoiceValue.Add(c.ArtifactID);
      }

      //compare previous and current choices
      if (!OrderedListOfIntEquals(previousChoiceValue, currentChoiceValue))
        fieldChanged = true;


      return fieldChanged;
    }

    private bool HasChangedNonChoiceField(int documentArtifactId, int sourceFieldArtifactId, int fieldTypeId)
    {
      var fieldChanged = false;
      var dbWorkspaceConnection = (DBContext)Helper.GetDBContext(Application.ArtifactID);

      //Obtain column name for Field
      var sqlFieldColumnName = $@"SELECT AVF.ColumnName FROM [EDDSDBO].[Field] F WITH(NOLOCK) INNER JOIN [EDDSDBO].[ArtifactViewField] AVF WITH(NOLOCK) ON F.ArtifactViewFieldID = AVF.ArtifactViewFieldID WHERE F.ArtifactID = {sourceFieldArtifactId}";
      var dsFieldColumnName = dbWorkspaceConnection.ExecuteSqlStatementAsDataSet(sqlFieldColumnName);

      if (dsFieldColumnName != null && dsFieldColumnName.Tables.Count > 0)
      {
        DataRow drFieldColumnName = dsFieldColumnName.Tables[0].Rows[0];

        if (drFieldColumnName["ColumnName"] != DBNull.Value)
        {
          //Obtain value for column name and document
          string sqlCheckForChange = $@"SELECT [{drFieldColumnName["ColumnName"]}] FieldValue FROM [EDDSDBO].[Document] WITH(NOLOCK) WHERE ArtifactID = {documentArtifactId}";
          var dsCheckForChange = dbWorkspaceConnection.ExecuteSqlStatementAsDataSet(sqlCheckForChange);

          if (dsCheckForChange != null && dsCheckForChange.Tables.Count > 0)
          {
            var drCheckForChange = dsCheckForChange.Tables[0].Rows[0];

            if (drCheckForChange["FieldValue"] != DBNull.Value)
            {
              switch (fieldTypeId)
              {
                case FtYesNo:
                  var dbFieldValue = (bool?)drCheckForChange["FieldValue"];
                  var dNewBoolean = (bool?)ActiveArtifact.Fields[sourceFieldArtifactId].Value.Value;

                  if (dNewBoolean == null)
                  {
                    if ((dbFieldValue != null))
                    {
                      fieldChanged = true;
                    }
                  }
                  else
                  {
                    if (dbFieldValue != dNewBoolean)
                    {
                      fieldChanged = true;
                    }
                  }
                  break;
              }
            }
            else
            {
              string sql = $@"SELECT [{drFieldColumnName["ColumnName"]}] FROM [EDDSDBO].[Document] D WITH(NOLOCK) WHERE D.ArtifactID = {documentArtifactId}";
              var objPreviousValue = dbWorkspaceConnection.ExecuteSqlStatementAsScalar(sql);
              var objCurrentValue = ActiveArtifact.Fields[sourceFieldArtifactId].Value.Value;

              if (fieldTypeId != FtYesNo) return false;
              if (!Nullable.Equals(ObjectToNullableOfBoolean(objPreviousValue),
                ObjectToNullableOfBoolean(objCurrentValue)))
                fieldChanged = true;
            }
          }
        }
      }

      return fieldChanged;
    }

    private static bool? ObjectToNullableOfBoolean(object value)
    {
      if (value == DBNull.Value || value == null)
      {
        return new bool?();
      }
      return Convert.ToBoolean(value);
    }

    private static bool OrderedListOfIntEquals(List<int> listA, List<int> listB)
    {
      if (listA.Count == listB.Count)
      {
        for (var i = 0; i <= listA.Count - 1; i++)
        {
          if (listA[i] != listB[i])
          {
            return false;
          }
        }
      }
      else
      {
        return false;
      }
      return true;
    }

    public override FieldCollection RequiredFields
    {
      get
      {
        FieldCollection retVal = new FieldCollection();
        return retVal;
      }
    }
  }
}
