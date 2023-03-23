package action

type JobInfo struct {
	JobID       string `json:"jobId,omitempty"`
	JobName     string `json:"jobName,omitempty"`
	RunId       string `json:"runId,omitempty"`
	JobOwnerId  string `json:"jobOwnerId,omitempty"`
	TriggerType string `json:"triggerType,omitempty"`
}

func JobInfoFromContext(context map[string]string) *JobInfo {
	if v, ok := context["jobId"]; ok {
		return &JobInfo{
			JobID:       v,
			JobName:     context["jobName"],
			RunId:       context["runId"],
			JobOwnerId:  context["jobOwnerId"],
			TriggerType: context["jobTriggerType"],
		}
	}
	return nil
}
