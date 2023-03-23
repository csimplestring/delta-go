package deltago

import (
	"github.com/csimplestring/delta-go/action"
	"github.com/csimplestring/delta-go/internal/util/parquet"
	"github.com/fraugster/parquet-go/floor/interfaces"
)

type actionMarshaller struct {
	a *action.SingleAction
}

func (p *actionMarshaller) MarshalParquet(obj interfaces.MarshalObject) error {

	if p.a.Txn != nil {
		return parquetMarshalTxn(p.a.Txn, obj.AddField("txn").Group())
	}
	if p.a.Add != nil {
		return parquetMarshalAdd(p.a.Add, obj.AddField("add").Group())
	}
	if p.a.Remove != nil {
		return parquetMarshalRemove(p.a.Remove, obj.AddField("remove").Group())
	}
	if p.a.MetaData != nil {
		return parquetMarshalMetadata(p.a.MetaData, obj.AddField("metaData").Group())
	}
	if p.a.Protocol != nil {
		return parquetMarshaProtocol(p.a.Protocol, obj.AddField("protocol").Group())
	}
	if p.a.Cdc != nil {
		return parquetMarshalCDC(p.a.Cdc, obj.AddField("cdc").Group())
	}
	if p.a.CommitInfo != nil {
		return parquetMarshalCommitInfo(p.a.CommitInfo, obj.AddField("commitInfo").Group())
	}

	return nil
}

func (p *actionMarshaller) UnmarshalParquet(obj interfaces.UnmarshalObject) error {
	data := obj.GetData()
	if _, ok := data["txn"]; ok {
		p.a.Txn = &action.SetTransaction{}
		return parquetUnmarshalTxn(p.a.Txn, obj)
	}
	if _, ok := data["add"]; ok {
		p.a.Add = &action.AddFile{}
		return parquetUnmarshalAdd(p.a.Add, obj)
	}
	if _, ok := data["remove"]; ok {
		p.a.Remove = &action.RemoveFile{}
		return parquetUnmarshalRemove(p.a.Remove, obj)
	}
	if _, ok := data["metaData"]; ok {
		p.a.MetaData = &action.Metadata{}
		return parquetUnmarshalMetadata(p.a.MetaData, obj)
	}
	if _, ok := data["protocol"]; ok {
		p.a.Protocol = &action.Protocol{}
		return parquetUnmarshaProtocol(p.a.Protocol, obj)
	}
	if _, ok := data["cdc"]; ok {
		p.a.Cdc = &action.AddCDCFile{}
		return parquetUnmarshalCDC(p.a.Cdc, obj)
	}
	if _, ok := data["commitInfo"]; ok {
		p.a.CommitInfo = &action.CommitInfo{}
		return parquetUnmarshalCommitInfo(p.a.CommitInfo, obj)
	}
	return nil
}

func parquetMarshalTxn(txn *action.SetTransaction, obj interfaces.MarshalObject) error {
	if len(txn.AppId) > 0 {
		obj.AddField("appId").SetByteArray([]byte(txn.AppId))
	}
	if txn.Version >= 0 {
		obj.AddField("version").SetInt64(txn.Version)
	}
	if txn.LastUpdated != nil {
		obj.AddField("lastUpdated").SetInt64(*txn.LastUpdated)
	}
	return nil
}

func parquetUnmarshalTxn(txn *action.SetTransaction, obj interfaces.UnmarshalObject) error {
	g, err := obj.GetField("txn").Group()
	if err != nil {
		return err
	}

	if err := parquet.UnmarshalString(g, "appId", func(s string) { txn.AppId = s }); err != nil {
		return err
	}
	if err := parquet.UnmarshalInt64(g, "version", func(s int64) { txn.Version = s }); err != nil {
		return err
	}
	if err := parquet.UnmarshalInt64(g, "lastUpdated", func(s int64) { txn.LastUpdated = &s }); err != nil {
		return err
	}

	return nil
}

func parquetMarshalAdd(add *action.AddFile, obj interfaces.MarshalObject) error {
	obj.AddField("path").SetByteArray([]byte(add.Path))
	obj.AddField("dataChange").SetBool(add.DataChange)

	parquet.MarshalMap(obj, "partitionValues", add.PartitionValues)

	obj.AddField("size").SetInt64(add.Size)
	obj.AddField("modificationTime").SetInt64(add.ModificationTime)

	if len(add.Stats) > 0 {
		obj.AddField("stats").SetByteArray([]byte(add.Stats))
	}
	if len(add.Tags) > 0 {
		parquet.MarshalMap(obj, "tags", add.Tags)
	}
	return nil
}

func parquetUnmarshalAdd(add *action.AddFile, obj interfaces.UnmarshalObject) error {
	g, err := obj.GetField("add").Group()
	if err != nil {
		return err
	}

	if err := parquet.UnmarshalString(g, "path", func(s string) { add.Path = s }); err != nil {
		return err
	}
	if err := parquet.UnmarshalBool(g, "dataChange", func(s bool) { add.DataChange = s }); err != nil {
		return err
	}
	if err := parquet.UnmarshalInt64(g, "size", func(s int64) { add.Size = s }); err != nil {
		return err
	}
	if err := parquet.UnmarshalInt64(g, "modificationTime", func(s int64) { add.ModificationTime = s }); err != nil {
		return err
	}
	if err := parquet.UnmarshalMap(g, "partitionValues", func(m map[string]string) { add.PartitionValues = m }); err != nil {
		return err
	}
	if err := parquet.UnmarshalString(g, "stats", func(s string) { add.Stats = s }); err != nil {
		return err
	}
	if err := parquet.UnmarshalMap(g, "tags", func(m map[string]string) { add.Tags = m }); err != nil {
		return err
	}

	return nil
}

func parquetMarshalRemove(rm *action.RemoveFile, obj interfaces.MarshalObject) error {
	obj.AddField("path").SetByteArray([]byte(rm.Path))
	obj.AddField("dataChange").SetBool(rm.DataChange)
	if rm.DeletionTimestamp != nil {
		obj.AddField("deletionTimestamp").SetInt64(*rm.DeletionTimestamp)
	}
	obj.AddField("extendedFileMetadata").SetBool(rm.ExtendedFileMetadata)
	parquet.MarshalMap(obj, "partitionValues", rm.PartitionValues)
	if rm.Size != nil {
		obj.AddField("size").SetInt64(*rm.Size)
	}
	parquet.MarshalMap(obj, "tags", rm.Tags)
	return nil
}

func parquetUnmarshalRemove(rm *action.RemoveFile, obj interfaces.UnmarshalObject) error {
	g, err := obj.GetField("remove").Group()
	if err != nil {
		return err
	}

	if err := parquet.UnmarshalString(g, "path", func(s string) { rm.Path = s }); err != nil {
		return err
	}
	if err := parquet.UnmarshalBool(g, "dataChange", func(s bool) { rm.DataChange = s }); err != nil {
		return err
	}
	if err := parquet.UnmarshalInt64(g, "deletionTimestamp", func(s int64) { rm.DeletionTimestamp = &s }); err != nil {
		return err
	}
	if err := parquet.UnmarshalBool(g, "extendedFileMetadata", func(s bool) { rm.ExtendedFileMetadata = s }); err != nil {
		return err
	}
	if err := parquet.UnmarshalMap(g, "partitionValues", func(m map[string]string) { rm.PartitionValues = m }); err != nil {
		return err
	}
	if err := parquet.UnmarshalInt64(g, "size", func(s int64) { rm.Size = &s }); err != nil {
		return err
	}
	if err := parquet.UnmarshalMap(g, "tags", func(m map[string]string) { rm.Tags = m }); err != nil {
		return err
	}
	return nil
}

func parquetMarshalMetadata(meta *action.Metadata, obj interfaces.MarshalObject) error {
	obj.AddField("id").SetByteArray([]byte(meta.ID))
	obj.AddField("name").SetByteArray([]byte(meta.Name))
	obj.AddField("description").SetByteArray([]byte(meta.Description))

	format := obj.AddField("format").Group()
	format.AddField("provider").SetByteArray([]byte(meta.Format.Proviver))
	parquet.MarshalMap(format, "options", meta.Format.Options)

	obj.AddField("schemaString").SetByteArray([]byte(meta.SchemaString))
	parquet.MarshalList(obj, "partitionColumns", meta.PartitionColumns)
	parquet.MarshalMap(obj, "configuration", meta.Configuration)
	if meta.CreatedTime != nil {
		obj.AddField("createdTime").SetInt64(*meta.CreatedTime)
	}
	return nil
}

func parquetUnmarshalMetadata(meta *action.Metadata, obj interfaces.UnmarshalObject) error {
	g, err := obj.GetField("metaData").Group()
	if err != nil {
		return err
	}

	if err := parquet.UnmarshalString(g, "id", func(s string) { meta.ID = s }); err != nil {
		return err
	}
	if err := parquet.UnmarshalString(g, "name", func(s string) { meta.Name = s }); err != nil {
		return err
	}
	if err := parquet.UnmarshalString(g, "description", func(s string) { meta.Description = s }); err != nil {
		return err
	}
	if err := parquet.UnmarshalString(g, "schemaString", func(s string) { meta.SchemaString = s }); err != nil {
		return err
	}
	if err := parquet.UnmarshalMap(g, "configuration", func(m map[string]string) { meta.Configuration = m }); err != nil {
		return err
	}
	if err := parquet.UnmarshalInt64(g, "createdTime", func(s int64) { meta.CreatedTime = &s }); err != nil {
		return err
	}

	format, err := g.GetField("format").Group()
	if err != nil {
		return err
	}
	if err := parquet.UnmarshalString(format, "provider", func(s string) { meta.Format.Proviver = s }); err != nil {
		return err
	}
	if err := parquet.UnmarshalMap(format, "options", func(m map[string]string) { meta.Format.Options = m }); err != nil {
		return err
	}

	if err := parquet.UnmarshalList(g, "partitionColumns", func(s []string) { meta.PartitionColumns = s }); err != nil {
		return err
	}

	return nil
}

func parquetMarshaProtocol(p *action.Protocol, obj interfaces.MarshalObject) error {
	obj.AddField("minReaderVersion").SetInt32(p.MinReaderVersion)
	obj.AddField("minWriterVersion").SetInt32(p.MinWriterVersion)
	return nil
}

func parquetUnmarshaProtocol(p *action.Protocol, obj interfaces.UnmarshalObject) error {
	g, err := obj.GetField("protocol").Group()
	if err != nil {
		return err
	}
	if err := parquet.UnmarshalInt32(g, "minReaderVersion", func(s int32) { p.MinReaderVersion = s }); err != nil {
		return err
	}
	if err := parquet.UnmarshalInt32(g, "minWriterVersion", func(s int32) { p.MinWriterVersion = s }); err != nil {
		return err
	}
	return nil
}

func parquetMarshalCDC(add *action.AddCDCFile, obj interfaces.MarshalObject) error {
	obj.AddField("path").SetByteArray([]byte(add.Path))
	parquet.MarshalMap(obj, "partitionValues", add.PartitionValues)
	obj.AddField("size").SetInt64(add.Size)

	if len(add.Tags) > 0 {
		parquet.MarshalMap(obj, "tags", add.Tags)
	}
	return nil
}

func parquetUnmarshalCDC(add *action.AddCDCFile, obj interfaces.UnmarshalObject) error {
	g, err := obj.GetField("cdc").Group()
	if err != nil {
		return err
	}

	if err := parquet.UnmarshalString(g, "path", func(s string) { add.Path = s }); err != nil {
		return err
	}
	if err := parquet.UnmarshalInt64(g, "size", func(s int64) { add.Size = s }); err != nil {
		return err
	}
	if err := parquet.UnmarshalMap(g, "partitionValues", func(m map[string]string) { add.PartitionValues = m }); err != nil {
		return err
	}
	if err := parquet.UnmarshalMap(g, "tags", func(m map[string]string) { add.Tags = m }); err != nil {
		return err
	}

	return nil
}

func parquetMarshalCommitInfo(add *action.CommitInfo, obj interfaces.MarshalObject) error {
	if add.Version != nil {
		obj.AddField("version").SetInt64(*add.Version)
	}
	obj.AddField("timestamp").SetInt64(add.Timestamp)
	if add.UserID != nil {
		obj.AddField("userId").SetByteArray([]byte(*add.UserID))
	}
	if add.UserName != nil {
		obj.AddField("userName").SetByteArray([]byte(*add.UserName))
	}
	obj.AddField("operation").SetByteArray([]byte(add.Operation))
	parquet.MarshalMap(obj, "operationParameters", add.OperationParameters)
	if add.ClusterId != nil {
		obj.AddField("clusterId").SetByteArray([]byte(*add.ClusterId))
	}
	if add.ReadVersion != nil {
		obj.AddField("readVersion").SetInt64(*add.ReadVersion)
	}
	if add.IsolationLevel != nil {
		obj.AddField("isolationLevel").SetByteArray([]byte(*add.IsolationLevel))
	}
	if add.IsBlindAppend != nil {
		obj.AddField("isBlindAppend").SetBool(*add.IsBlindAppend)
	}
	parquet.MarshalMap(obj, "operationMetrics", add.OperationMetrics)
	if add.UserMetadata != nil {
		obj.AddField("userMetadata").SetByteArray([]byte(*add.UserMetadata))
	}
	if add.EngineInfo != nil {
		obj.AddField("engineInfo").SetByteArray([]byte(*add.EngineInfo))
	}

	if add.Job != nil {
		job := obj.AddField("job").Group()
		job.AddField("jobId").SetByteArray([]byte(add.Job.JobID))
		job.AddField("jobName").SetByteArray([]byte(add.Job.JobName))
		job.AddField("jobOwnerId").SetByteArray([]byte(add.Job.JobOwnerId))
		job.AddField("runId").SetByteArray([]byte(add.Job.RunId))
		job.AddField("triggerType").SetByteArray([]byte(add.Job.TriggerType))
	}

	if add.Notebook != nil {
		n := obj.AddField("notebook").Group()
		n.AddField("notebookId").SetByteArray([]byte(add.Notebook.NotebookId))
	}
	return nil
}

func parquetUnmarshalCommitInfo(add *action.CommitInfo, obj interfaces.UnmarshalObject) error {
	g, err := obj.GetField("commitInfo").Group()
	if err != nil {
		return err
	}

	if err := parquet.UnmarshalInt64(g, "version", func(s int64) { add.Version = &s }); err != nil {
		return err
	}
	if err := parquet.UnmarshalInt64(g, "timestamp", func(s int64) { add.Timestamp = s }); err != nil {
		return err
	}
	if err := parquet.UnmarshalString(g, "userId", func(s string) { add.UserID = &s }); err != nil {
		return err
	}
	if err := parquet.UnmarshalString(g, "userName", func(s string) { add.UserName = &s }); err != nil {
		return err
	}
	if err := parquet.UnmarshalString(g, "operation", func(s string) { add.Operation = s }); err != nil {
		return err
	}
	if err := parquet.UnmarshalMap(g, "operationParameters", func(m map[string]string) { add.OperationParameters = m }); err != nil {
		return err
	}
	if err := parquet.UnmarshalString(g, "clusterId", func(s string) { add.ClusterId = &s }); err != nil {
		return err
	}
	if err := parquet.UnmarshalInt64(g, "readVersion", func(s int64) { add.ReadVersion = &s }); err != nil {
		return err
	}
	if err := parquet.UnmarshalBool(g, "isBlindAppend", func(s bool) { add.IsBlindAppend = &s }); err != nil {
		return err
	}
	if err := parquet.UnmarshalString(g, "isolationLevel", func(s string) { add.IsolationLevel = &s }); err != nil {
		return err
	}
	if err := parquet.UnmarshalMap(g, "operationMetrics", func(m map[string]string) { add.OperationMetrics = m }); err != nil {
		return err
	}
	if err := parquet.UnmarshalString(g, "userMetadata", func(s string) { add.UserMetadata = &s }); err != nil {
		return err
	}
	if err := parquet.UnmarshalString(g, "engineInfo", func(s string) { add.EngineInfo = &s }); err != nil {
		return err
	}

	if _, ok := g.GetData()["job"]; ok {
		job, err := g.GetField("job").Group()
		if err != nil {
			return err
		}
		add.Job = &action.JobInfo{}
		if err := parquet.UnmarshalString(job, "jobId", func(s string) { add.Job.JobID = s }); err != nil {
			return err
		}
		if err := parquet.UnmarshalString(job, "jobName", func(s string) { add.Job.JobName = s }); err != nil {
			return err
		}
		if err := parquet.UnmarshalString(job, "jobOwnerId", func(s string) { add.Job.JobOwnerId = s }); err != nil {
			return err
		}
		if err := parquet.UnmarshalString(job, "runId", func(s string) { add.Job.RunId = s }); err != nil {
			return err
		}
		if err := parquet.UnmarshalString(job, "triggerType", func(s string) { add.Job.TriggerType = s }); err != nil {
			return err
		}
	}

	if _, ok := g.GetData()["notebook"]; ok {
		n, err := g.GetField("notebook").Group()
		if err != nil {
			return err
		}
		add.Notebook = &action.NotebookInfo{}
		if err := parquet.UnmarshalString(n, "notebookId", func(s string) { add.Notebook.NotebookId = s }); err != nil {
			return err
		}
	}

	return nil
}
